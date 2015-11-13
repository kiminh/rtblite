package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	"github.com/op/go-logging"
)

type BlockItem struct {
	PackageName string
	Carrier     string
}

type BlockList struct {
	updateTime     time.Time
	blockedCarrier map[BlockItem]bool
	configure      *Configure
	logger         *logging.Logger
}

func NewBlockList(configure *Configure, logger *logging.Logger) *BlockList {
	t := &BlockList{
		configure:      configure,
		blockedCarrier: make(map[BlockItem]bool, 0),
		logger:         logger,
	}
	t.Load()
	return t
}

func (bl *BlockList) Load() error {
	start := time.Now()
	bl.updateTime = time.Now()
	// default
	file, err := os.Open(bl.configure.BlockListPath)
	if err != nil {
		return err
	}
	defer file.Close()
	blockedContent, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	blocked := make([][]string, 0)
	err = json.Unmarshal(blockedContent, &blocked)
	if err != nil {
		return err
	}
	newBlockList := make(map[BlockItem]bool)
	for _, value := range blocked {
		if len(value) >= 2 {
			newBlockList[BlockItem{value[0], value[1]}] = true
		}
	}
	bl.blockedCarrier = newBlockList
	bl.logger.Notice("block list updated, %v item(s) loaded, time spent %v", len(bl.blockedCarrier), time.Now().Sub(start))
	return err
}

/*
func main() {
	c := NewConfigure()
	c.LoadFromFile("rtblite.conf")
	b := NewBlockList(c, rotatelogger.NewLogger("", "", ""))
	fmt.Println(b.blockedCarrier)
}
*/
