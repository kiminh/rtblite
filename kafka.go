package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/op/go-logging"
	"strconv"
)

type KafkaWrapper struct {
	configure *Configure
	producer  sarama.AsyncProducer
	logger    *logging.Logger
}

func NewKafkaWrapper(configure *Configure, logger *logging.Logger) (*KafkaWrapper, error) {
	if !configure.KafkaEnable {
		return &KafkaWrapper{
			configure: configure,
		}, nil
	}
	brokers := strings.Split(configure.KafkaBrokers, ",")
	config := sarama.NewConfig()
	// config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &KafkaWrapper{
		configure: configure,
		producer:  producer,
		logger:    logger,
	}, nil
}

func (kw *KafkaWrapper) Log(topic string, message string) {
	if !kw.configure.KafkaEnable {
		return
	}
	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	kw.producer.Input() <- producerMessage
}

func GetEventKafkaMessage(req *ParsedRequest, event string, record *Inventory) string {
	// carrier只取第一个
	carrier, err := strconv.Atoi(strings.Split(req.M, ",")[0])
	if err != nil {
		carrier = -1
	}

	message := []interface{}{
		time.Now().UTC().Format("2006-01-02 15:04:05Z"),
		req.PlacementId,
		record.AdType,
		HiveHash(record.IconUrl),
		record.PackageName,
		carrier,
		NanIfEmpty(req.IpLib.CountryCode),
		NanIfEmpty(req.OsVersion),
		NanIfEmpty(req.ClientVersion),
		req.Network,
		req.Adgroup,
		NanIfEmpty(req.Cid),
	}
	switch event {
	case "impression":
		message = append(message, 0, 1, 0, 0, 0)
	case "click":
		message = append(message, 0, 0, 1, 0, 0)
	case "td_postback":
		message = append(message, 0, 0, 0, 1, record.Price)
	}
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", message...)
}

func GetReqeustKafkaMessage(req *ParsedRequest) string {
	// carrier只取第一个
	carrier, err := strconv.Atoi(strings.Split(req.M, ",")[0])
	if err != nil {
		carrier = -1
	}

	message := []interface{}{
		time.Now().UTC().Format("2006-01-02 15:04:05Z"),
		req.PlacementId,
		carrier,
		NanIfEmpty(req.IpLib.CountryCode),
		NanIfEmpty(req.OsVersion),
		NanIfEmpty(req.ClientVersion),
		req.Network,
		req.Adgroup,
		1,
		len(req.Creatives),
	}
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", message...)
}
