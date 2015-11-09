package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"
)

type RankTable struct {
	updateTime time.Time
	rank       map[string]int
	rankFile   string
}

func NewRankTable(rankFile string) (*RankTable, error) {
	t := &RankTable{
		rankFile: rankFile,
		rank:     make(map[string]int),
	}
	if err := t.Load(); err != nil {
		return nil, err
	}
	return t, nil
}

func (rt *RankTable) Len() int { return len(rt.rank) }

func (rt *RankTable) Load() error {
	newTable := make(map[string]int)
	file, err := os.Open(rt.rankFile)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	rankList := make([]string, 0)
	err = json.Unmarshal(content, &rankList)
	if err != nil {
		return err
	}
	for index, value := range rankList {
		newTable[value] = index
	}
	rt.rank = newTable
	rt.updateTime = time.Now()
	return nil
}
