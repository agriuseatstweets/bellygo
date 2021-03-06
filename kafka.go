package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"

	"log"
)

type KafkaConsumer struct {
	Consumer *kafka.Consumer
}

func NewKafkaConsumer(topic string, brokers string, group string) KafkaConsumer {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    brokers,
		"group.id":             group,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   "false",
		"max.poll.interval.ms": "960000",
	})

	if err != nil {
		// TODO: handle in error channel?
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)
	return KafkaConsumer{c}
}

func (consumer KafkaConsumer) Consume(n int, timeout time.Duration, errs chan error) chan *BellyData {
	messages := make(chan *BellyData)
	c := consumer.Consumer

	// runs until n messages consumed
	go func() {
		defer close(messages)
		for i := 1; i <= n; i++ {

			msg, err := c.ReadMessage(timeout)

			if err != nil {
				if e, ok := err.(kafka.Error); ok && e.Code() == kafka.ErrTimedOut {
					break
				}
				errs <- err
				break
			}

			// TODO: make NewBellyData with msg.Key also
			dat, err := NewBellyData(msg.Value)
			if err != nil {
				log.Printf("Belly could not parse message as BellyData: %s", err)
				continue
			}
			messages <- dat
		}
	}()

	return messages
}
