package main

import (
	"strings"

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
