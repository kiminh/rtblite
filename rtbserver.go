package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
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
	return &RtbLite{
		geoDb:        geoDb,
		cache:        NewInventoryCache(configure, logger),
		logger:       logger,
		redisWrapper: NewRedisWrapper(configure, logger),
		producer:     producer,
		configure:    configure,
	}, nil
}

func (rl *RtbLite) CacheUpdateLoop(d time.Duration) error {
	if err := rl.cache.Load(); err != nil {
		return err
	}
	timer := time.NewTimer(d)
	go func() {
		for range timer.C {
			rl.cache.Load()
			timer.Reset(d)
		}
	}()
	return nil
}

func VersionToInt(versionString string) int {
	splited := strings.Split(versionString, ".")
	version := 0
	for i := len(splited) - 1; i >= 0; i-- {
		if value, err := strconv.Atoi(splited[i]); err == nil {
			version += value * int(math.Pow10(len(splited)-1-i))
		}
	}
	return version
}

type ParsedRequest struct {
	Limit         int          `json:"limit"`
	PlacementId   string       `json:"placement_id"`
	L             string       `json:"l"`
	M             string       `json:"m"`
	Ip            string       `json:"ip"`
	Cid           string       `json:"cid"`
	OsVersion     string       `json:"os_version"`
	ClientVersion string       `json:"client_version"`
	Network       string       `json:"network"`
	Cc            string       `json:"cc"`
	Hp            string       `json:"hp"`
	P             string       `json:"p"`
	C             string       `json:"c"`
	Adgroup       string       `json:"adgroup_id"`
	Creatives     []*Inventory `json:"creatives"`
	Id            string       `json:"id"`

	countryCode  string
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
	r.countryCode = rl.geoDb.GetLocationByIP(r.Ip).CountryCode
	return r
}

func (rl *RtbLite) SelectByPackage(req *ParsedRequest, creatives *InventoryQueue, count int) []*Inventory {
	req.Adgroup = "4"
	selectedCreatives := make([]*Inventory, 0)
	for _, record := range *creatives {
		if req.OsVersionNum < record.minOsNum || req.OsVersionNum > record.maxOsNum {
			continue
		}
		selectedCreatives = append(selectedCreatives, record)
		if len(selectedCreatives) >= count {
			break
		}
	}
	return selectedCreatives
}

func GetParam(index int, req *ParsedRequest, record *Inventory) string {
	param := ""
	price, err := strconv.ParseFloat(record.price, 64)
	if err != nil {
		price = 0
	}
	if strings.ToLower(record.adType) == "bigtree6" {
		param = fmt.Sprintf("5_2_%v_%v_%v_%v_%v_%v_%v_%v-%v",
			req.Cid, record.adType, req.Cc, req.Hp, req.P, req.C,
			req.ClientVersion, req.Id, index)
		param = fmt.Sprintf("s2=%v&s3=%v&s4=%v&s5=c7caa578-763f-44de-8673-8f1bfbb3c3c8&s1=",
			record.packageName, price, param)
	} else {
		param += fmt.Sprintf("5_1_%v_%v_%v_%v_%v_%v_%v_%v_%v_%v-%v",
			req.Cid, req.Cc, record.adType, req.Hp, req.P, req.C,
			req.ClientVersion, record.packageName, price, req.Id, index)
		switch strings.ToLower(record.adType) {
		case "bigtree1":
			param = "subid1=" + param + "&subid2=&subid3=&m.gaid="
		case "bigtree2":
			param = "p1=subid1&v1=" + param + "&p2=subid2&v2=&p3=subid3&v3="
		case "bigtree3":
			param = "postback=" + url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(param)))
		case "bigtree4":
			param = "aff_sub=" + param + "&aff_sub2=&aff_sub3=&aff_sub5="
		case "bigtree5":
			param = "cv1n=subid1&cv1v=" + param + "&cv2n=subid2&cv2v=&cv3n=subid3&cv3v="
		case "bigtree7":
			param = "clickId=" + param
		case "bigtree8":
			param = "q=" + param
		case "bigtree9":
			param = "aff_sub=" + param + "&aff_sub2=&aff_sub3=&aff_sub4=9-1"
		case "bigtree10":
			param = "dv1=" + param + "&dv2=&dv3="
		default:
			break
		}
	}
	return param
}

func (rl *RtbLite) Augment(req *ParsedRequest, creatives InventoryQueue) {
	if frequencies, err := rl.redisWrapper.GetFrequency(req, creatives); err != nil {
		rl.logger.Warning("redis error: %v", err.Error())
		return
	} else {
		for index, value := range creatives {
			value.Frequency = frequencies[index]
		}
	}
}

func NanIfEmpty(value string) string {
	if value == "" {
		return "NAN"
	} else {
		return value
	}
}

func GetReqeustKafkaMessage(req *ParsedRequest) string {
	carrier := strings.Split(req.M, ",")[0]
	if carrier == "" {
		carrier = "-1"
	}
	message := []interface{}{
		time.Now().UTC().Format("2006-01-02 15:04:05Z"),
		req.PlacementId,
		carrier,
		NanIfEmpty(req.countryCode),
		NanIfEmpty(req.OsVersion),
		NanIfEmpty(req.ClientVersion),
		NanIfEmpty(req.Network),
		req.Adgroup,
		1,
		len(req.Creatives),
	}
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", message...)
}

func HiveHash(content string) int {
	hash := 0
	for _, b := range content {
		hash = hash*31 + int(b)
		hash &= 0x7FFFFFFF
	}
	return hash
}

func GetEventKafkaMessage(req *ParsedRequest, event string, record *Inventory) string {
	carrier := strings.Split(req.M, ",")[0]
	if carrier == "" {
		carrier = "-1"
	}
	message := []interface{}{
		time.Now().UTC().Format("2006-01-02 15:04:05Z"),
		req.PlacementId,
		record.adType,
		HiveHash(record.iconUrl),
		record.packageName,
		carrier,
		NanIfEmpty(req.countryCode),
		NanIfEmpty(req.OsVersion),
		NanIfEmpty(req.ClientVersion),
		NanIfEmpty(req.Network),
		req.Adgroup,
	}
	switch event {
	case "response":
		message = append(message, 1, 0, 0, 0, 0)
	case "impression":
		message = append(message, 0, 1, 0, 0, 0)
	case "click":
		message = append(message, 0, 0, 1, 0, 0)
	case "td_postback":
		message = append(message, 0, 0, 0, 1, record.price)
	}
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", message...)
}

func (rl *RtbLite) Request(rw http.ResponseWriter, req *http.Request) {
	parsed := rl.Parse(req)
	filteredByCountry, ok := rl.cache.cacheByCountry[parsed.countryCode]
	if !ok {
		io.WriteString(rw, "")
		return
	}
	rl.Augment(parsed, filteredByCountry)
	creativesToReturn := rl.SelectByPackage(parsed, &filteredByCountry, parsed.Limit)
	ret := make([]string, 0)
	count := 0
	for index, record := range creativesToReturn {
		creativeId := fmt.Sprintf("%v-%v", parsed.Id, index)
		callbackParam := GetParam(index, parsed, record)
		clickUrl := record.clickUrl + "&" + callbackParam
		clickTracker := "http://" + rl.configure.HttpAddress + "/click?final_url=" +
			url.QueryEscape(clickUrl) + "&param=" + creativeId
		impressionTracker := "http://" + rl.configure.HttpAddress + "/impression?param=" + creativeId
		ret = append(ret, fmt.Sprintf(`{ "bundle_id": "%v",  "click_url": "%v", "creative_url": "%v", "icon_url": "%v", "impression_url": "%v", "title": "%v" }`,
			record.packageName, clickTracker, record.bannerUrl, record.iconUrl, impressionTracker, record.label))
		count += 1
	}
	response := `{"ad": [` + strings.Join(ret, ",") + `], "error_code": 0, "error_message": "success"}`
	io.WriteString(rw, response)

	rl.redisWrapper.SaveRequest(parsed, creativesToReturn)
	rl.producer.Log("request_v3", GetReqeustKafkaMessage(parsed))
}

func SplitId(param string) (string, int, error) {
	splited := strings.Split(param, "-")
	id, index := splited[0], splited[1]
	creativeIndex, err := strconv.Atoi(index)
	return id, creativeIndex, err
}

func (rl *RtbLite) Impression(rw http.ResponseWriter, req *http.Request) {
	param := req.URL.Query().Get("param")
	id, index, err := SplitId(param)
	if err != nil {
		rl.logger.Error("fail to get id from param: %v", err.Error())
		return
	}
	parsed, err := rl.redisWrapper.GetRequest(id)
	if err != nil {
		rl.logger.Error(err.Error())
	} else {
		rl.logger.Debug("%v", *parsed)
		rl.redisWrapper.IncrFrequency(parsed, parsed.Creatives[index].AdId)
		record := &Inventory{}
		if err := rl.cache.FetchOne(parsed.Creatives[index].AdId, record); err != nil {
			rl.logger.Error(err.Error())
			return
		}
		rl.logger.Debug("%v", *parsed)
		rl.redisWrapper.SetExpire(param, 86400)
		rl.producer.Log("impression_v3", GetEventKafkaMessage(parsed, "impression", record))
	}
}

func (rl *RtbLite) Click(rw http.ResponseWriter, req *http.Request) {
	param := req.URL.Query().Get("param")
	id, index, err := SplitId(param)
	if err != nil {
		rl.logger.Error("fail to get id from param: %v", err.Error())
		return
	}
	parsed, err := rl.redisWrapper.GetRequest(id)
	if err != nil {
		rl.logger.Error(err.Error())
	} else {
		record := &Inventory{}
		if err := rl.cache.FetchOne(parsed.Creatives[index].AdId, record); err != nil {
			rl.logger.Error(err.Error())
			return
		}
		rl.logger.Debug("%v", *parsed)
		rl.redisWrapper.SetExpire(param, 86400*3)
		rl.producer.Log("click_v3", GetEventKafkaMessage(parsed, "click", record))
	}
}

func (rl *RtbLite) Conversion(rw http.ResponseWriter, req *http.Request) {
	param := req.URL.Query().Get("param")
	id, index, err := SplitId(param)
	if err != nil {
		rl.logger.Error("fail to get id from param: %v", err.Error())
		return
	}
	parsed, err := rl.redisWrapper.GetRequest(id)
	if err != nil {
		rl.logger.Error(err.Error())
	} else {
		record := &Inventory{}
		if err := rl.cache.FetchOne(parsed.Creatives[index].AdId, record); err != nil {
			rl.logger.Error(err.Error())
			return
		}
		rl.logger.Debug("%v", *parsed)
		rl.redisWrapper.SetExpire(param, 86400/2)
		rl.producer.Log("click_v3", GetEventKafkaMessage(parsed, "td_postback", record))
	}
}
