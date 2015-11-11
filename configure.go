package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type Configure struct {
	HttpAddress     string `default:"0.0.0.0:8705"`
	CallbackAddress string `default:"0.0.0.0:8705"`
	ClickAddress    string `default:"0.0.0.0:8705"`

	MySqlAddress        string `default:"localhost:3306"`
	MySqlUser           string `default:"root"`
	MySqlPassword       string `default:""`
	MySqlDatabase       string `default:""`
	MysqlUpdateInterval int    `default:"60"`

	KafkaEnable          bool   `default:"true"`
	KafkaBrokers         string `default:"localhost:9092"`
	KafkaRequestTopic    string `default:"request"`
	KafkaImressionTopic  string `default:"impression"`
	KafkaClickTopic      string `default:"click"`
	KafkaConversionTopic string `default:"td_postback"`

	RedisFrequencyAddress string `default:"localhost:6379"`
	RedisFrequencyPrefix  string `default:"fr:"`
	RedisFrequencyPerId   int    `default:"5"`

	RedisJoinAddress           string `default:"localhost:6379"`
	RedisJoinPrefix            string `default:"param:"`
	RedisJoinRequestTimeout    int    `default:"43200"`
	RedisJoinImpressionTimeout int    `default:"86400"`
	RedisJoinClickTimeout      int    `default:"259200"`
	RedisJoinConversionTimeout int    `default:"43200"`

	LogLevel string `default:"debug"`
	LogDir   string `default:""`

	ProfilerEnable   bool `default:"true"`
	ProfilerInterval int  `default:"10"`

	TrafficRandom        int  `default:"80"`
	FillRankedWithRandom bool `default:"true"`

	ModelDataSaveDir string `default:"./"`

	RankTablePath         string `default:"adrank.json"`
	RankByAdunitTablePath string `default:"adrank_by_adunit.json"`
}

func NewConfigure() *Configure {
	configure := &Configure{}
	configure.InitWithDefault()
	return configure
}

func (c *Configure) InitWithDefault() {
	refType := reflect.TypeOf(c).Elem()
	refValue := reflect.ValueOf(c).Elem()
	for i := 0; i < refType.NumField(); i++ {
		t := refType.Field(i)
		v := refValue.Field(i)
		switch v.Kind() {
		case reflect.String:
			v.SetString(t.Tag.Get("default"))
		case reflect.Int:
			if i, err := strconv.ParseInt(t.Tag.Get("default"), 10, 32); err == nil {
				v.SetInt(i)
			}
		case reflect.Bool:
			if i, err := strconv.ParseBool(strings.ToLower(t.Tag.Get("default"))); err == nil {
				v.SetBool(i)
			}
		}
		// strconv.ParseBool()
	}
}

func (c *Configure) LoadFromFile(file string) error {
	configFile, err := os.Open(file)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(configFile)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, &c); err != nil {
		return err
	}
	return nil
}

func (c *Configure) SaveToFile(file string) error {
	configFile, err := os.Create(file)
	if err != nil {
		return err
	}
	content, err := json.MarshalIndent(c, "", "    ")
	if err != nil {
		return err
	}
	if _, err := configFile.Write(content); err != nil {
		return err
	}
	return nil
}

func (c *Configure) String() string {
	example, err := json.MarshalIndent(c, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(example)
}
