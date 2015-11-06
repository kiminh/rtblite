package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/op/go-logging"
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
