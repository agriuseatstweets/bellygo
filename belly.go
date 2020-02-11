package main

import (
	"time"
	"log"
	"fmt"
	"sync"
	"compress/gzip"
	"context"
	"github.com/caarlos0/env/v6"
	"cloud.google.com/go/storage"
)


func merge(cs ...<-chan error) <-chan error {
    var wg sync.WaitGroup
    out := make(chan error)

    output := func(c <-chan error) {
        for n := range c {
            out <- n
        }
        wg.Done()
    }
    wg.Add(len(cs))
    for _, c := range cs {
        go output(c)
    }

    go func() {
        wg.Wait()
        close(out)
    }()
    return out
}


type TweetWriter struct {
	gz *gzip.Writer
	gc *storage.Writer
}

type TweetWriteChan struct {
	In chan<- *TweetData
	Errs <-chan error
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

func NewTweetWriteChan(bkt *storage.BucketHandle, date string) *TweetWriteChan {
	timestamp := time.Now().Format(time.RFC3339)
	object := fmt.Sprintf("datestamp=%v/%v.json.gz", date, timestamp)
	gc := bkt.Object(object).NewWriter(context.Background())
	gz := gzip.NewWriter(gc)

	inch := make(chan *TweetData)
	errs := make(chan error)
	writer := TweetWriter{gz, gc}

	go func() {
		defer close(errs)
		for tweet := range inch {
			if err := writer.WriteTweet(tweet); err != nil {
				log.Print("Error while writing tweet: \n")
				log.Println(err)
				errs <- err
			}
		}

		// close writer when inchan closes
		if err := writer.Close(); err != nil {
			panic(err)
		}
	}()

	return &TweetWriteChan{inch, errs}
}


func WriteTweets(bkt *storage.BucketHandle, tweets chan *TweetData) {
	writers := make(map[string] *TweetWriteChan)

	for tweet := range tweets {
		t, err := tweet.Tweet.CreatedAtTime()
		if err != nil {
			panic(err)
		}
		d := t.Format("2006-01-02")
		w, ok := writers[d]

		if ok == false {
			w = NewTweetWriteChan(bkt, d)
			writers[d] = w
		}

		w.In <- tweet
	}

	// close in chans and block
	// until all the writer out chans close
	// we ignore write errors in the out chans
	outs := [] <-chan error{}
	for _, w := range writers {
		close(w.In)
		outs = append(outs, w.Errs)
	}
	for _ = range merge(outs...) {}
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

	WriteTweets(bkt, tweets)
	tp, err := c.Consumer.Commit()
	if err != nil {
		panic(err)
	}

	log.Printf("Committed topic partition: %v", tp)
}
