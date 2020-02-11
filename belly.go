package main

import (
	"time"
	"log"
	"fmt"
	"compress/gzip"
	"context"
	"github.com/caarlos0/env/v6"
	"cloud.google.com/go/storage"
)


type TweetWriter struct {
	gz *gzip.Writer
	gc *storage.Writer
}

func (t *TweetWriter) WriteTweet(tweet *TweetData) error {
	_, err := t.gz.Write(tweet.Bytes)
	if err != nil {
		return err
	}
	_, err = t.gz.Write([]byte{'\n'})
	return err
}

func (t *TweetWriter) Close() error {
	if err := t.gz.Close(); err != nil {
		return err
	}
	if err := t.gc.Close(); err != nil {
		return err
	}
	return nil
}

func NewTweetWriter(bkt *storage.BucketHandle, date string) *TweetWriter {
	timestamp := time.Now().Format(time.RFC3339)
	object := fmt.Sprintf("datestamp=%v/%v.json.gz", date, timestamp)
	gc := bkt.Object(object).NewWriter(context.Background())
	gz := gzip.NewWriter(gc)
	return &TweetWriter{gz, gc}
}


func WriteTweets(bkt *storage.BucketHandle, tweets chan *TweetData) error {
	data := make(map[string] []*TweetData)

	for tweet := range tweets {
		t, err := tweet.Tweet.CreatedAtTime()
		if err != nil {
			panic(err)
		}
		d := t.Format("2006-01-02")
		data[d] = append(data[d], tweet)
	}

	for d, tws := range data {
		writer := NewTweetWriter(bkt, d)
		for _, tw := range tws {
			if err := writer.WriteTweet(tw); err != nil {
				log.Print("Error while writing tweet: \n")
				log.Println(err)
			}
		}

		if err := writer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Belly failed with error: %v", e)
}

type Config struct {
	Size int `env:"BELLY_SIZE,required"`
	Location string `env:"BELLY_LOCATION,required"`
	KafkaBrokers string `env:"KAFKA_BROKERS,required"`
	Topic string `env:"BELLY_TOPIC,required"`
}

func getConfig() Config {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		fmt.Printf("%+v\n", err)
	}
	return cfg
}

func main() {
	cnf := getConfig()

	client, _ := storage.NewClient(context.Background())
	bkt := client.Bucket(cnf.Location)

	errs := make(chan error)
	go monitor(errs)
	c := NewKafkaConsumer(cnf.Topic, cnf.KafkaBrokers)
	tweets := c.Consume(cnf.Size, errs)

	err:= WriteTweets(bkt, tweets)
	if err != nil {
		panic(err)
	}

	tp, err := c.Consumer.Commit()
	if err != nil {
		panic(err)
	}

	log.Printf("Committed topic partition: %v", tp)
}
