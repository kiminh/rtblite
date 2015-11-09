package main

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func VersionToInt(versionString string) int {
	version := 0
	split := strings.Split(versionString, ".")
	safeConvert := func(str string) int {
		if value, err := strconv.Atoi(str); err == nil {
			return value
		} else {
			fmt.Printf("err: %s %s", err, err.(*strconv.NumError).Num)
			return 0
		}
	}

	switch len(split) {
	case 0:
	default:
		version = safeConvert(split[2])
		fallthrough
	case 2:
		version += 100 * safeConvert(split[1])
		fallthrough
	case 1:
		version += 10000 * safeConvert(split[0])
	}

	return version
}

func GetParam(index int, req *ParsedRequest, record *Inventory) string {
	param := ""
	price, err := strconv.ParseFloat(record.Price, 64)
	if err != nil {
		price = 0
	}
	price /= 1e6

	if strings.ToLower(record.AdType) == "bigtree6" {
		param = fmt.Sprintf("5_2_%v_%v_%v_%v_%v_%v_%v_%v-%v",
			req.Cid, record.AdType, req.Cc, req.Hp, req.P, req.C,
			req.ClientVersion, req.Id, index)
		param = fmt.Sprintf("s2=%v&s3=%v&s4=%v&s5=c7caa578-763f-44de-8673-8f1bfbb3c3c8&s1=",
			record.PackageName, price, param)
	} else {
		param += fmt.Sprintf("5_1_%v_%v_%v_%v_%v_%v_%v_%v_%v_%v-%v",
			req.Cid, req.Cc, record.AdType, req.Hp, req.P, req.C,
			req.ClientVersion, record.PackageName, price, req.Id, index)
		switch strings.ToLower(record.AdType) {
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

func HiveHash(content string) int {
	hash := 0
	for _, b := range content {
		hash = hash*31 + int(b)
		hash &= 0x7FFFFFFF
	}
	return hash
}

func NanIfEmpty(value string) string {
	if value == "" {
		return "NAN"
	} else {
		return value
	}
}

func SplitId(param string) (string, int, error) {
	splited := strings.Split(param, "-")
	id, index := splited[0], splited[1]
	creativeIndex, err := strconv.Atoi(index)
	return id, creativeIndex, err
}
