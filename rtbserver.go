package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/nranchev/go-libGeoIP"
	"github.com/op/go-logging"
	"github.com/yangzhao28/go.uuid"
	"github.com/yangzhao28/rotatelogger"
)

type RtbLite struct {
	geoDb        *libgeo.GeoIP
	cache        *InventoryCache
	logger       *logging.Logger
	redisWrapper *RedisWrapper
	producer     *KafkaWrapper
	configure    *Configure
	profiler     *Profiler
	saveFile     *rotatelogger.Rotator

	r *rand.Rand
}

func NewRtbLite(configure *Configure) (*RtbLite, error) {
	geoDb, err := libgeo.Load("/usr/share/GeoIP/GeoLiteCity.dat")
	if err != nil {
		return nil, err
	}
	logger := rotatelogger.NewLogger("rtblite", configure.LogDir, configure.LogLevel)
	producer, err := NewKafkaWrapper(configure, logger)
	if err != nil {
		return nil, err
	}
	var saveFile *rotatelogger.Rotator = nil
	if configure.ModelDataSaveDir != "" {
		saveFile = &rotatelogger.Rotator{}
		saveFile.Create(path.Join(configure.ModelDataSaveDir, "model.save"), rotatelogger.DailyRotation)
	}
	cache, err := NewInventoryCache(configure, logger)
	if err != nil {
		return nil, err
	}
	return &RtbLite{
		geoDb:        geoDb,
		cache:        cache,
		logger:       logger,
		redisWrapper: NewRedisWrapper(configure, logger),
		producer:     producer,
		configure:    configure,
		profiler:     NewProfiler(configure, logger),
		saveFile:     saveFile,
		r:            rand.New(rand.NewSource(time.Now().Unix())),
	}, nil
}

func (rl *RtbLite) RunProfiler() {
	go rl.profiler.Collect()
}

func (rl *RtbLite) CacheUpdateLoop() error {
	if err := rl.cache.Load(); err != nil {
		return err
	}
	timer := time.NewTimer(time.Duration(rl.configure.MysqlUpdateInterval) * time.Second)
	go func() {
		for range timer.C {
			rl.cache.Load()
			timer.Reset(time.Duration(rl.configure.MysqlUpdateInterval) * time.Second)
		}
	}()
	return nil
}

type ParsedRequest struct {
	Limit         int                  `json:"limit"`
	PlacementId   string               `json:"placement_id"`
	L             string               `json:"l"`
	M             string               `json:"m"`
	Ip            string               `json:"ip"`
	Cid           string               `json:"cid"`
	OsVersion     string               `json:"os_version"`
	ClientVersion string               `json:"client_version"`
	Network       string               `json:"network"`
	Cc            string               `json:"cc"`
	Hp            string               `json:"hp"`
	P             string               `json:"p"`
	C             string               `json:"c"`
	Adgroup       string               `json:"adgroup_id"`
	Creatives     []*InventoryForRedis `json:"creatives"`
	Id            string               `json:"id"`

	Location     *libgeo.Location `json:"location"`
	OsVersionNum int
}

func (rl *RtbLite) Parse(req *http.Request) *ParsedRequest {
	r := &ParsedRequest{}
	if limit, err := strconv.Atoi(req.URL.Query().Get("limit")); err == nil {
		r.Limit = limit
	} else {
		r.Limit = 8
	}
	r.PlacementId = req.URL.Query().Get("placement_id")
	r.L = req.URL.Query().Get("l")
	r.M = req.URL.Query().Get("m")
	r.Ip = req.URL.Query().Get("ip")
	r.Cid = req.URL.Query().Get("cid")
	r.OsVersion = req.URL.Query().Get("os_version")
	r.OsVersionNum = VersionToInt(r.OsVersion)
	r.ClientVersion = req.URL.Query().Get("client_version")
	r.Network = req.URL.Query().Get("network")
	r.Cc = req.URL.Query().Get("cc")
	r.Hp = req.URL.Query().Get("hp")
	r.P = req.URL.Query().Get("p")
	r.C = req.URL.Query().Get("c")

	r.Id = uuid.NewV4().Hex()
	r.Location = rl.geoDb.GetLocationByIP(r.Ip)
	if r.Location == nil {
		r.Location = &libgeo.Location{}
	}
	return r
}

func (rl *RtbLite) SelectByPackage(req *ParsedRequest, creatives *InventoryCollection, count int) []*Inventory {
	req.Adgroup = "4"
	selectedCreatives := make([]*Inventory, 0)
	lastPackageName := ""
	for _, record := range creatives.Data {
		if req.OsVersionNum < record.MinOsNum || req.OsVersionNum > record.MaxOsNum {
			continue
		}
		if record.Frequency > rl.configure.RedisFrequencyPerId {
			continue
		}
		if lastPackageName != record.PackageName {
			selectedCreatives = append(selectedCreatives, record)
			if len(selectedCreatives) >= count {
				break
			}
			lastPackageName = record.PackageName
		}
	}
	return selectedCreatives
}

func (rl *RtbLite) SelectByRandom(req *ParsedRequest, creatives *InventoryCollection, count int) []*Inventory {
	req.Adgroup = "1"
	r := rand.New(rand.NewSource(time.Now().Unix()))
	randomSelect := r.Perm(creatives.Len())
	uniqueCreatives := make(map[string]*Inventory)
	for _, index := range randomSelect {
		record := creatives.Data[index]
		if req.OsVersionNum < record.MinOsNum || req.OsVersionNum > record.MaxOsNum {
			continue
		}
		if record.Frequency > rl.configure.RedisFrequencyPerId {
			continue
		}
		if _, ok := uniqueCreatives[record.PackageName]; !ok {
			uniqueCreatives[record.PackageName] = record
			if len(uniqueCreatives) >= count {
				break
			}
		}
	}
	selectedCreatives := make([]*Inventory, 0)
	for _, record := range uniqueCreatives {
		selectedCreatives = append(selectedCreatives, record)
	}
	return selectedCreatives
}

func (rl *RtbLite) Augment(req *ParsedRequest, creatives *InventoryCollection) {
	if frequencies, err := rl.redisWrapper.GetFrequency(req, creatives); err != nil {
		rl.logger.Warning("redis error: %v", err.Error())
		return
	} else {
		for index, value := range creatives.Data {
			value.Frequency = frequencies[index]
		}
	}
}

func (rl *RtbLite) Request(rw http.ResponseWriter, req *http.Request) {
	start := time.Now()
	defer rl.profiler.OnRequest(time.Now().Sub(start).Seconds())

	parsed := rl.Parse(req)
	filteredByCountry, ok := rl.cache.cacheByCountry[parsed.Location.CountryCode]
	if !ok {
		io.WriteString(rw, "")
		return
	}
	rl.Augment(parsed, filteredByCountry)

	chooseCreative := rl.r.Intn(100)
	var creativesToReturn []*Inventory
	if chooseCreative < rl.configure.TrafficRandom {
		creativesToReturn = rl.SelectByRandom(parsed, filteredByCountry, parsed.Limit)
	} else {
		creativesToReturn = rl.SelectByPackage(parsed, filteredByCountry, parsed.Limit)
	}
	ret := make([]string, 0)
	for index, record := range creativesToReturn {
		creativeId := fmt.Sprintf("%v-%v", parsed.Id, index)
		clickTracker := "http://" + rl.configure.ClickAddress + "/click?final_url=" +
			url.QueryEscape(record.ClickUrl+"&"+GetParam(index, parsed, record)) + "&param=" + creativeId
		impressionTracker := "http://" + rl.configure.CallbackAddress + "/impression?param=" + creativeId
		ret = append(ret, fmt.Sprintf(`{ "bundle_id": "%v",  "click_url": "%v", "creative_url": "%v", "icon_url": "%v", "impression_url": "%v", "title": "%v" }`,
			record.PackageName, clickTracker, record.BannerUrl, record.IconUrl, impressionTracker, record.Label))
	}
	response := `{"ad": [` + strings.Join(ret, ",") + `], "error_code": 0, "error_message": "success"}`
	io.WriteString(rw, response)

	// 提前把结果发出去，后续操作可以慢慢做
	go func() {
		rl.redisWrapper.SaveRequest(parsed, creativesToReturn, rl.configure.RedisRequestTimeout)
		rl.producer.Log(rl.configure.KafkaRequestTopic, GetReqeustKafkaMessage(parsed))
	}()
}

func (rl *RtbLite) Impression(rw http.ResponseWriter, req *http.Request) {
	defer rl.profiler.OnImpression()

	param := req.URL.Query().Get("param")
	id, index, err := SplitId(param)
	if err != nil {
		rl.logger.Error("fail to get id from param: %v", err.Error())
		return
	}
	response := `{"error_code": 0, "error_message": "success"}`
	io.WriteString(rw, response)
	// 提前把结果发出去，后续操作可以慢慢做
	go func() {
		time.Sleep(1 * time.Second) // 为了防止redis时序颠倒取不到id
		parsed, err := rl.redisWrapper.GetRequest(id)
		if err != nil {
			rl.logger.Error(err.Error())
		} else {
			record := &Inventory{}
			if err := rl.cache.FetchOne(parsed.Creatives[index].AdId, record); err != nil {
				rl.logger.Error(err.Error())
				return
			}
			rl.redisWrapper.IncrFrequency(parsed, record.ModelSign1)
			rl.redisWrapper.SetExpire(param, rl.configure.RedisImpressionTimeout)
			rl.producer.Log(rl.configure.KafkaImressionTopic, GetEventKafkaMessage(parsed, "impression", record))

			if rl.saveFile != nil {
				if data, err := GetModelDataLog(parsed, record, "impression"); err != nil {
					rl.logger.Warning(err.Error())
				} else {
					if _, err := rl.saveFile.WriteString(fmt.Sprintln(string(data))); err != nil {
						rl.logger.Warning(err.Error())
					}
				}
			}
		}
	}()
}

func (rl *RtbLite) Click(rw http.ResponseWriter, req *http.Request) {
	defer rl.profiler.OnClick()
	param := req.URL.Query().Get("param")
	id, index, err := SplitId(param)
	if err != nil {
		rl.logger.Error("fail to get id from param: %v", err.Error())
		return
	}

	finalUrl := req.URL.Query().Get("final_url")
	if len(finalUrl) > 0 {
		http.Redirect(rw, req, finalUrl, 302)
	} else {
		response := `{"error_code": 0, "error_message": "success"}`
		io.WriteString(rw, response)
	}

	// 提前把结果发出去，后续操作可以慢慢做
	go func() {
		time.Sleep(1 * time.Second) // 为了防止redis时序颠倒取不到id
		parsed, err := rl.redisWrapper.GetRequest(id)
		if err != nil {
			rl.logger.Error(err.Error())
		} else {
			record := &Inventory{}
			if err := rl.cache.FetchOne(parsed.Creatives[index].AdId, record); err != nil {
				rl.logger.Error(err.Error())
				return
			}
			rl.redisWrapper.SetExpire(param, rl.configure.RedisClickTimeout)
			rl.producer.Log(rl.configure.KafkaClickTopic, GetEventKafkaMessage(parsed, "click", record))
			if rl.saveFile != nil {
				if data, err := GetModelDataLog(parsed, record, "impression"); err != nil {
					if _, err := rl.saveFile.WriteString(fmt.Sprintln(string(data))); err != nil {
						rl.logger.Warning(err.Error())
					}
				}
			}
		}
	}()
}

func (rl *RtbLite) Conversion(rw http.ResponseWriter, req *http.Request) {
	param := req.URL.Query().Get("param")
	id, index, err := SplitId(param)
	if err != nil {
		rl.logger.Error("fail to get id from param: %v", err.Error())
		return
	}

	response := `{"error_code": 0, "error_message": "success"}`
	io.WriteString(rw, response)

	// 提前把结果发出去，后续操作可以慢慢做
	go func() {
		time.Sleep(1 * time.Second) // 为了防止redis时序颠倒取不到id
		parsed, err := rl.redisWrapper.GetRequest(id)
		if err != nil {
			rl.logger.Error(err.Error())
		} else {
			record := &Inventory{}
			if err := rl.cache.FetchOne(parsed.Creatives[index].AdId, record); err != nil {
				rl.logger.Error(err.Error())
				return
			}
			rl.redisWrapper.SetExpire(param, rl.configure.RedisConversionTimeout)
			rl.producer.Log(rl.configure.KafkaConversionTopic, GetEventKafkaMessage(parsed, "td_postback", record))
			if rl.saveFile != nil {
				if data, err := GetModelDataLog(parsed, record, "impression"); err != nil {
					if _, err := rl.saveFile.WriteString(fmt.Sprintln(string(data))); err != nil {
						rl.logger.Warning(err.Error())
					}
				}
			}
		}
	}()
}

func (rl *RtbLite) UpdateRank(rw http.ResponseWriter, req *http.Request) {
	if err := rl.cache.UpdateRankTable(); err != nil {
		io.WriteString(rw, fmt.Sprintf("failed, %v", err.Error()))
	} else {
		io.WriteString(rw, fmt.Sprintf("success, %v item(s) loaded\n", rl.cache.rankTable.Len()))
	}
}

func (rl *RtbLite) GetRank(rw http.ResponseWriter, req *http.Request) {
	rt := rl.cache.GetRankTable()
	encoder := json.NewEncoder(rw)
	encoder.Encode(rt.rank)
}
