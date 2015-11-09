package main

import (
	"encoding/json"
	"time"
)

type IpLib struct {
	IpHashLevel1 int    `json:"ip_hash_level1"`
	IpHashLevel2 int    `json:"ip_hash_level2"`
	IpHashLevel3 int    `json:"ip_hash_level3"`
	CountryCode  string `json:"country_code"`
}

// {"connection_type": 9, "c": "100000", "user_id": "a473bad6f66321cc3e516a617a09e2ca", "ip_lib": {"ip_hash_level1": 2128, "ip_hash_level3": 2048976, "country_code": "BR", "ip_hash_level2": 66092}, "language": "pt_BR", "p": "8-2", "cc": "BR", "ip": "177.17.9.107", "hp": "com.apusapps.launcher", "adgroup_id": "1", "selected_creative": {"status": "offline", "click_url": "http://tr.pubnative.net/click/bulk?aid=1006719&u=%26offer_id%3D5188%26source%3D16555&pid=1639324&aaid=1003493&nid=23", "ad_id": 1980744661, "banner_url": "https://lh4.ggpht.com/qL6d5NqDFKZq1d0g1qyiocBxotisLnWtaKHsSAb3Jvpgvt50hysITv9Fn-bR7wfeS-5n", "package_name": "com.machinezone.gow", "user_frequency": 0, "icon_url": "http://cdn.pubnative.net/games/icons/001/003/493/dimension_150x150.jpg?20151021230347", "price": 360000, "ad_type": "bigtree4", "label": "Game of War - Fire Age", "max_os": "", "extensions": "{}", "min_os_num": 0, "min_os": "2.3", "max_os_num": 999999, "model_sign1": 1256253024, "country": "BR", "id": 30610}, "os_version": "4.3", "carrier": "72402", "limit": 10, "timestamp": 1446825535.902425, "request_id": "15ba2a9729f741e8b395537f4d440f77", "app_version": "134", "event": "impression", "adunit_id": "1"}

type ModelData struct {
	ConnectionType   int        `json:"connection_type"`
	C                string     `json:"c"`
	UserId           string     `json:"user_id"`
	IpLib            *IpLib     `json:"ip_lib"`
	Lauguage         string     `json:"lauguage"`
	P                string     `json:"P"`
	Cc               string     `json:"cc"`
	Ip               string     `json:"ip"`
	Hp               string     `json:"hp"`
	AdgroupId        string     `json:"adgroup_id"`
	SelectedCreative *Inventory `json:"selected_creative"`
	AdUnitId         string     `json:"adunit_id"`
	OsVersion        string     `json:"os_version"`
	Carrier          string     `json:"carrier"`
	Limit            int        `json:"limit"`
	Timestamp        int64      `json:"timestamp"`
	RequestId        string     `json:"request_id"`
	AppVersion       string     `json:"app_version"`
	Event            string     `json:"event"`
}

func GetModelDataLog(req *ParsedRequest, record *Inventory, event string) ([]byte, error) {
	data := ModelData{
		ConnectionType:   req.Network,
		C:                req.C,
		UserId:           req.Cid,
		IpLib:            req.IpLib,
		Lauguage:         req.L,
		P:                req.P,
		Cc:               req.Cc,
		Ip:               req.Ip,
		Hp:               req.Hp,
		AdgroupId:        req.Adgroup,
		SelectedCreative: record,

		AdUnitId:   req.PlacementId,
		OsVersion:  req.OsVersion,
		Carrier:    req.M,
		Limit:      req.Limit,
		Timestamp:  time.Now().Unix(),
		RequestId:  req.Id,
		AppVersion: req.ClientVersion,
		Event:      event,
	}
	jsonData, err := json.Marshal(data)
	return jsonData, err
}
