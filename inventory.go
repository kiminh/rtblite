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

var priorityTable = map[string]int{
	"net.peakgames.mobile.spades.android":    0,
	"com.ebay.annunci":                       1,
	"com.mcdonalds.app":                      2,
	"com.king.alphabettysaga":                3,
	"jp.coloplni.wcatus":                     4,
	"my.com.fourgames.baby":                  5,
	"com.tonaton":                            6,
	"com.subway.mobile.subwayapp03":          7,
	"com.netmarble.goldenagegb":              8,
	"net.peakgames.mobile.canakokey.android": 9,
	"air.com.sgn.bookoflife.gp":              10,
	"com.sgn.pandapop.gp":                    11,
	"ru.beeline.services":                    12,
	"com.wego.android":                       13,
	"jp.coloplni.scs":                        14,
	"com.mercadopago.wallet":                 15,
	"vn.com.mobagame.ldxy":                   16,
	"com.bukalapak.android":                  17,
	"org.altruist.BajajExperia":              18,
	"com.chaatz":                             19,
	"com.apt.publish.dsm":                    20,
	"com.flipkart.android":                   21,
	"com.app.tokobagus.betterb":              22,
	"my.ezjoy.coh":                           23,
	"com.deliveryclub":                       24,
	"com.netmarble.sknightsgb":               25,
	"com.opera.mini.native":                  26,
}

type Inventory struct {
	id          int
	AdId        int `json:"ad_id"`
	packageName string
	iconUrl     string
	label       string
	clickUrl    string
	price       string
	maxOs       string
	minOs       string
	bannerUrl   string
	country     string
	adType      string
	status      string
	modelSign1  int
	extension   string
	minOsNum    int
	maxOsNum    int
	ts          []byte

	Frequency int `json:"user_frequency"`
}

type InventoryQueue []*Inventory

var r = rand.New(rand.NewSource(time.Now().Unix()))

func (iq InventoryQueue) Len() int { return len(iq) }
func (iq InventoryQueue) Less(i, j int) bool {
	iIndex, iOk := priorityTable[iq[i].packageName]
	if !iOk {
		iIndex = r.Intn(1000) + len(priorityTable)
	}
	jIndex, jOk := priorityTable[iq[j].packageName]
	if !jOk {
		jIndex = r.Intn(1000) + len(priorityTable)
	}

	if iIndex == jIndex {
		return iq[i].price > iq[j].price
	}
	return iIndex < jIndex
}

func (iq InventoryQueue) Swap(i, j int) { iq[i], iq[j] = iq[j], iq[i] }

type InventoryCache struct {
	databaseHandler *sql.DB
	configure       *Configure
	cacheByCountry  map[string]InventoryQueue

	lock   sync.Mutex
	logger *logging.Logger
}

func NewInventoryCache(configure *Configure, logger *logging.Logger) *InventoryCache {
	return &InventoryCache{
		configure: configure,
		logger:    logger,
	}
}

func (inv *InventoryCache) Reconnect() (err error) {
	return
}

type RowScanner interface {
	Scan(dest ...interface{}) error
}

func RowToObject(row RowScanner, record *Inventory) error {
	return row.Scan(&record.id, &record.AdId, &record.packageName,
		&record.iconUrl, &record.label, &record.clickUrl,
		&record.price, &record.maxOs, &record.minOs,
		&record.bannerUrl, &record.country, &record.adType,
		&record.status, &record.modelSign1, &record.extension,
		&record.minOsNum, &record.maxOsNum, &record.ts)
}

func (inv *InventoryCache) FetchOne(adId int, record *Inventory) error {
	row := inv.databaseHandler.QueryRow("select * from inventory where status='online' and ad_id=?", adId)
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
	rows, err := inv.databaseHandler.Query("select * from inventory where status='online'")
	if err != nil {
		// 释放掉下次重连
		inv.logger.Warning("fail to execute sql: %v", err.Error())
		inv.databaseHandler = nil
		return err
	}
	sqlTimeSpent := meter.TimeElapsed()
	countryMap := make(map[string]InventoryQueue)
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
		if _, ok := countryMap[record.country]; !ok {
			countryMap[record.country] = make(InventoryQueue, 0)
		}
		countryMap[record.country] = append(countryMap[record.country], &record)
		countLoaded += 1
	}
	recordTimeSpent := meter.TimeElapsed() - sqlTimeSpent
	inv.logger.Notice("%v record loaded, %v countries, %v errors", countLoaded, len(countryMap), errorCount)
	for _, queue := range countryMap {
		sort.Sort(queue)
	}
	sortTimeSpent := meter.TimeElapsed() - recordTimeSpent
	uniqueMap := make(map[string]InventoryQueue)
	for countryCode, queue := range countryMap {
		newQueue := InventoryQueue{}
		var lastRecord *Inventory = nil
		for _, record := range queue {
			if lastRecord != nil && lastRecord.packageName == record.packageName {
				continue
			} else {
				newQueue = append(newQueue, record)
				lastRecord = record
			}
		}
		uniqueMap[countryCode] = newQueue
	}
	uniqTimeSpent := meter.TimeElapsed() - recordTimeSpent
	inv.cacheByCountry = uniqueMap
	totallySpent := meter.TimeElapsed()
	inv.logger.Notice("cache updated, totallySpent = %v,  sqlTimeSpent = %v, recordTimeSpent = %v, sortTimeSpent = %v, uniqTimeSpent = %v",
		totallySpent, sqlTimeSpent, recordTimeSpent, sortTimeSpent, uniqTimeSpent)
	return nil
}
