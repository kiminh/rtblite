package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

type Rank struct {
	PackageName string
	AdType      string
	Country     string
}

type RankTable struct {
	updateTime   time.Time
	rankDefault  map[Rank]int
	rankByAdunit map[string]map[Rank]int
	configure    *Configure
}

func NewRankTable(configure *Configure) (*RankTable, error) {
	t := &RankTable{
		configure:    configure,
		rankDefault:  make(map[Rank]int),
		rankByAdunit: make(map[string]map[Rank]int),
	}
	if err := t.Load(); err != nil {
		return nil, err
	}
	return t, nil
}

func (rt *RankTable) Load() error {
	rt.updateTime = time.Now()
	// default
	fileDefault, err := os.Open(rt.configure.RankTablePath)
	if err != nil {
		return err
	}
	defer fileDefault.Close()
	rankContentDefault, err := ioutil.ReadAll(fileDefault)
	if err != nil {
		return err
	}
	rankDefault := make([][]string, 0)
	err = json.Unmarshal(rankContentDefault, &rankDefault)
	if err != nil {
		return err
	}
	newRankTableDefault := make(map[Rank]int)
	for index, value := range rankDefault {
		if len(value) == 2 {
			newRankTableDefault[Rank{value[0], value[1], ""}] = index
		} else if len(value) == 3 {
			newRankTableDefault[Rank{value[0], value[1], value[2]}] = index
		} else {
			fmt.Println(value, "is not a valid rank item")
		}
	}
	rt.rankDefault = newRankTableDefault

	fileByAdunit, err := os.Open(rt.configure.RankByAdunitTablePath)
	if err != nil {
		fmt.Println("fail to load RankByAdunit File:", err.Error(), ", skip")
		return nil
	}
	defer fileByAdunit.Close()
	rankContentByAdunit, err := ioutil.ReadAll(fileByAdunit)
	if err != nil {
		return err
	}
	rankByAdunit := make(map[string][][]string, 0)
	err = json.Unmarshal(rankContentByAdunit, &rankByAdunit)
	if err != nil {
		return err
	}
	newRankTableByAdunit := make(map[string]map[Rank]int)
	for adunit, rank := range rankByAdunit {
		newRankTableByAdunit[adunit] = make(map[Rank]int)
		for index, value := range rank {
			if len(value) == 2 {
				newRankTableByAdunit[adunit][Rank{value[0], value[1], ""}] = index
			} else if len(value) == 3 {
				newRankTableByAdunit[adunit][Rank{value[0], value[1], value[2]}] = index
			} else {
				fmt.Println(value, "is not a valid rank item")
			}
		}
	}
	rt.rankByAdunit = newRankTableByAdunit
	return err
}

/*
func main() {
	c := NewConfigure()
	c.LoadFromFile("rtblite.conf")
	r, _ := NewRankTable(c)
	r.Load()
}
*/
