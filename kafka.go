package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)


type KafkaConsumer struct {
	Consumer *kafka.Consumer
}


func NewKafkaConsumer(topic string, brokers string) KafkaConsumer {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "bellygo",
		"auto.offset.reset": "earliest",
		"enable.auto.commit": "false",
		"max.poll.interval.ms": "960000",
	})

	if err != nil {
		// TODO: handle in error channel?
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)
	return KafkaConsumer{c}
}

func (consumer KafkaConsumer) Consume (n int, errs chan error) chan *TweetData {
	messages := make(chan *TweetData)
	c := consumer.Consumer

	// runs until n messages consumed
	go func() {
		defer close(messages)
		for i := 1; i <= n; i++ {

			msg, err := c.ReadMessage(-1)

			if err != nil {
				errs <- err
				break
			}

			dat, err := NewTweetData(msg.Value)
			if err != nil {
				errs <- err
				break
			}
			messages <- dat
		}
	}()

	return messages
}
