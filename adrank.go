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
}

type RankTable struct {
	updateTime time.Time
	rank       map[Rank]int
	rankFile   string
}

func NewRankTable(rankFile string) (*RankTable, error) {
	t := &RankTable{
		rankFile: rankFile,
		rank:     make(map[Rank]int),
	}
	if err := t.Load(); err != nil {
		return nil, err
	}
	return t, nil
}

func (rt *RankTable) Len() int { return len(rt.rank) }

func (rt *RankTable) Load() error {
	newTable := make(map[Rank]int)
	file, err := os.Open(rt.rankFile)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	rankList := make([][]string, 0)
	err = json.Unmarshal(content, &rankList)
	if err != nil {
		return err
	}
	for index, value := range rankList {
		if len(value) == 2 {
			newTable[Rank{value[0], value[1]}] = index
		} else {
			fmt.Println(value, "is not a valid rank item")
		}
	}
	rt.rank = newTable
	rt.updateTime = time.Now()
	return nil
}
