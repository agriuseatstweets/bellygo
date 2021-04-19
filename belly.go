package main

import (
	"cloud.google.com/go/storage"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/segmentio/ksuid"
	"log"
	"sync"
	"time"
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

type BellyWriter struct {
	gz *gzip.Writer
	gc *storage.Writer
}

type WriteChan struct {
	In   chan<- *BellyData
	Errs <-chan error
}

func (t *BellyWriter) Write(dat *BellyData) error {
	_, err := t.gz.Write(dat.Value)
	if err != nil {
		return err
	}
	_, err = t.gz.Write([]byte{'\n'})
	return err
}

func (t *BellyWriter) Close() error {
	if err := t.gz.Close(); err != nil {
		return err
	}
	if err := t.gc.Close(); err != nil {
		return err
	}
	return nil
}

func NewWriteChan(bkt *storage.BucketHandle, date string) *WriteChan {
	filename := ksuid.New()

	// TODO: add source (topic) as a partitioned column...?
	object := fmt.Sprintf("datestamp=%v/%v.json.gz", date, filename)
	gc := bkt.Object(object).NewWriter(context.Background())
	gz := gzip.NewWriter(gc)

	inch := make(chan *BellyData)
	errs := make(chan error)
	writer := BellyWriter{gz, gc}

	go func() {
		defer close(errs)
		for dat := range inch {
			if err := writer.Write(dat); err != nil {
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

	return &WriteChan{inch, errs}
}

func WriteData(bkt *storage.BucketHandle, data chan *BellyData) {
	writers := make(map[string]*WriteChan)

	for d := range data {
		w, ok := writers[d.Key]

		if ok == false {
			w = NewWriteChan(bkt, d.Key)
			writers[d.Key] = w
		}

		w.In <- d
	}

	// close in chans and block
	// until all the writer out chans close
	// we ignore write errors in the out chans
	outs := []<-chan error{}
	for _, w := range writers {
		close(w.In)
		outs = append(outs, w.Errs)
	}
	for _ = range merge(outs...) {
	}
}

func monitor(errs <-chan error) {
	e := <-errs
	log.Fatalf("Belly failed with error: %v", e)
}

type Config struct {
	Size             int           `env:"BELLY_SIZE,required"`
	Location         string        `env:"BELLY_LOCATION,required"`
	KafkaBrokers     string        `env:"KAFKA_BROKERS,required"`
	KafkaPollTimeout time.Duration `env:"KAFKA_POLL_TIMEOUT,required"`
	Topic            string        `env:"BELLY_TOPIC,required"`
	Group            string        `env:"BELLY_GROUP,required"`
}

func getConfig() Config {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		panic(err)
	}
	return cfg
}

func handleCommitError(err error) bool {
	if err == nil {
		return true
	}

	if e, ok := err.(kafka.Error); ok && e.Code() == kafka.ErrNoOffset {
		log.Print("Belly consumed no messages and is committing no messages")
		return false
	}

	panic(err)
}

func main() {
	cnf := getConfig()

	client, _ := storage.NewClient(context.Background())
	bkt := client.Bucket(cnf.Location)

	errs := make(chan error)
	go monitor(errs)
	c := NewKafkaConsumer(cnf.Topic, cnf.KafkaBrokers, cnf.Group)
	data := c.Consume(cnf.Size, cnf.KafkaPollTimeout, errs)

	WriteData(bkt, data)

	tp, err := c.Consumer.Commit()
	ok := handleCommitError(err)
	if ok {
		log.Printf("Committed topic partition: %v", tp)
	}
}
