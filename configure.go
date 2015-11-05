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
	HttpAddress string `default:"0.0.0.0:8705"`

	MySqlAddress  string `default:"localhost:3306"`
	MySqlUser     string `default:"root"`
	MySqlPassword string `default:""`
	MySqlDatabase string `default:""`

	KafkaEnable          bool   `default:"true"`
	KafkaBrokers         string `default:"localhost:9092"`
	KafkaRequestTopic    string `default:"request"`
	KafkaImressionTopic  string `default:"impression"`
	KafkaClickTopic      string `default:"click"`
	KafkaConversionTopic string `default:"td_postback"`

	RedisAddress         string `default:"localhost:6379"`
	RedisCachePrefix     string `default:"param:"`
	RedisFrequencyPrefix string `default:"fr:"`

	LogLevel string `default:"debug"`
	LogDir   string `default:""`
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

func (c *Configure) String() string {
	example, err := json.MarshalIndent(c, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(example)
}
