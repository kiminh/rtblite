package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/op/go-logging"
)

type TimeMeter struct {
	begin time.Time
}

func NewTimeMeter() *TimeMeter {
	return &TimeMeter{
		begin: time.Now(),
	}
}

func (tm *TimeMeter) TimeElapsed() float64 {
	return float64(time.Now().UnixNano()-tm.begin.UnixNano()) / 1e9
}

type Inventory struct {
	Id          int    `json:"id"`
	AdId        int    `json:"ad_id"`
	PackageName string `json:"package_name"`
	IconUrl     string `json:"icon_url"`
	Label       string `json:"label"`
	ClickUrl    string `json:"click_url"`
	Price       string `json:"price"`
	MaxOs       string `json:"max_os"`
	MinOs       string `json:"min_os"`
	BannerUrl   string `json:"banner_url"`
	Country     string `json:"country"`
	AdType      string `json:"ad_type"`
	Status      string `json:"status"`
	ModelSign1  int    `json:"model_sign1"`
	Extension   string `json:"extension"`
	MinOsNum    int    `json:"min_os_num"`
	MaxOsNum    int    `json:"max_os_num"`
	Ts          []byte `json:"ts"`

	Frequency int `json:"user_frequency"`
}

type InventoryForRedis struct {
	AdId      int `json:"ad_id"`
	Frequency int `json:"user_frequency"`
}

type InventoryCollection struct {
	Data          []*Inventory
	PriorityTable *RankTable
	r             *rand.Rand
}

func NewInventoryCollection(rankTable *RankTable) *InventoryCollection {
	return &InventoryCollection{
		Data:          make([]*Inventory, 0),
		PriorityTable: rankTable,
		r:             rand.New(rand.NewSource(time.Now().Unix())),
	}
}

func (iq *InventoryCollection) Append(inv *Inventory) {
	iq.Data = append(iq.Data, inv)
}

func (iq InventoryCollection) Len() int { return len(iq.Data) }
func (iq InventoryCollection) Less(i, j int) bool {
	iIndex, iOk := iq.PriorityTable.rank[iq.Data[i].PackageName]
	if !iOk {
		iIndex = iq.r.Intn(1000) + iq.PriorityTable.Len()
	}
	jIndex, jOk := iq.PriorityTable.rank[iq.Data[j].PackageName]
	if !jOk {
		jIndex = iq.r.Intn(1000) + iq.PriorityTable.Len()
	}

	if iIndex == jIndex {
		return iq.Data[i].Price > iq.Data[j].Price
	}
	return iIndex < jIndex
}

func (iq InventoryCollection) Swap(i, j int) { iq.Data[i], iq.Data[j] = iq.Data[j], iq.Data[i] }

type InventoryCache struct {
	databaseHandler *sql.DB
	configure       *Configure
	cacheByCountry  map[string]*InventoryCollection
	rankTable       *RankTable

	lock   sync.Mutex
	logger *logging.Logger
}

func NewInventoryCache(configure *Configure, logger *logging.Logger) (*InventoryCache, error) {
	rankTable, err := NewRankTable(configure.RankTablePath)
	if err != nil {
		return nil, err
	}
	if err = rankTable.Load(); err != nil {
		return nil, err
	}
	return &InventoryCache{
		configure: configure,
		logger:    logger,
		rankTable: rankTable,
	}, nil
}

func (inv *InventoryCache) UpdateRankTable() error {
	inv.lock.Lock()
	defer inv.lock.Unlock()
	inv.logger.Notice("begin to update rank table")
	err := inv.rankTable.Load()
	if err != nil {
		inv.logger.Notice("updating interrupted, %v", err.Error())
	} else {
		inv.logger.Notice("updating done, %v item(s) loaded", inv.rankTable.Len())
	}
	return err
}

func (inv *InventoryCache) GetRankTable() *RankTable {
	inv.lock.Lock()
	defer inv.lock.Unlock()
	rankTable := inv.rankTable
	return rankTable
}

type RowScanner interface {
	Scan(dest ...interface{}) error
}

func RowToObject(row RowScanner, record *Inventory) error {
	return row.Scan(&record.Id, &record.AdId, &record.PackageName,
		&record.IconUrl, &record.Label, &record.ClickUrl,
		&record.Price, &record.MaxOs, &record.MinOs,
		&record.BannerUrl, &record.Country, &record.AdType,
		&record.Status, &record.ModelSign1, &record.Extension,
		&record.MaxOsNum, &record.MinOsNum, &record.Ts)
}

func (inv *InventoryCache) FetchOne(adId int, record *Inventory) error {
	row := inv.databaseHandler.QueryRow(`
		SELECT id, ad_id, package_name,
		       icon_url, label, click_url,
		       price, max_os, min_os,
		       banner_url, country, ad_type,
		       status, model_sign1, extensions,
		       max_os_num, min_os_num, ts
		FROM inventory
		WHERE status='online' AND ad_id=?
	`, adId)
	return RowToObject(row, record)
}

func (inv *InventoryCache) Load() error {
	inv.lock.Lock()
	defer inv.lock.Unlock()
	// 重连
	if inv.databaseHandler == nil {
		c := inv.configure
		dsn := fmt.Sprintf("%v:%v@tcp(%v)/%v",
			c.MySqlUser, c.MySqlPassword, c.MySqlAddress, c.MySqlDatabase)
		if conn, err := sql.Open("mysql", dsn); err != nil {
			inv.logger.Warning("fail to connect to mysql: %v", err.Error())
			return err
		} else {
			inv.databaseHandler = conn
		}
	}
	meter := NewTimeMeter()
	rows, err := inv.databaseHandler.Query(`
		SELECT id, ad_id, package_name,
		       icon_url, label, click_url,
		       price, max_os, min_os,
		       banner_url, country, ad_type,
		       status, model_sign1, extensions,
		       max_os_num, min_os_num, ts
		FROM inventory
		WHERE status='online'
	`)
	if err != nil {
		// 释放掉下次重连
		inv.logger.Warning("fail to execute sql: %v", err.Error())
		inv.databaseHandler = nil
		return err
	}
	sqlTimeSpent := meter.TimeElapsed()
	countryMap := make(map[string]*InventoryCollection)
	countLoaded := 0
	errorCount := 0
	for rows.Next() {
		record := Inventory{}
		err := RowToObject(rows, &record)
		if err != nil {
			if errorCount < 10 {
				inv.logger.Warning(err.Error())
			} else if errorCount == 10 {
				inv.logger.Warning("to many errors, ignored")
			}
			errorCount += 1
			continue
		}
		if _, ok := countryMap[record.Country]; !ok {
			countryMap[record.Country] = NewInventoryCollection(inv.rankTable)
		}
		countryMap[record.Country].Append(&record)
		countLoaded += 1
	}
	recordTimeSpent := meter.TimeElapsed() - sqlTimeSpent
	inv.logger.Notice("%v record loaded, %v countries, %v errors", countLoaded, len(countryMap), errorCount)
	for _, queue := range countryMap {
		sort.Sort(queue)
	}
	sortTimeSpent := meter.TimeElapsed() - recordTimeSpent
	//	uniqueMap := make(map[string]InventoryCollection)
	//	for countryCode, queue := range countryMap {
	//		newQueue := InventoryCollection{}
	//		var lastRecord *Inventory = nil
	//		for _, record := range queue {
	//			if lastRecord != nil && lastRecord.packageName == record.packageName {
	//				continue
	//			} else {
	//				newQueue = append(newQueue, record)
	//				lastRecord = record
	//			}
	//		}
	//		uniqueMap[countryCode] = newQueue
	//	}
	//	uniqTimeSpent := meter.TimeElapsed() - recordTimeSpent
	inv.cacheByCountry = countryMap
	totallySpent := meter.TimeElapsed()
	inv.logger.Notice("cache updated, totallySpent = %v,  sqlTimeSpent = %v, recordTimeSpent = %v, sortTimeSpent = %v",
		totallySpent, sqlTimeSpent, recordTimeSpent, sortTimeSpent)
	return nil
}
