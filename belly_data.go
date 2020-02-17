package main

import (
	"encoding/json"
    "github.com/dghubble/go-twitter/twitter"
)

type BellyData struct {
	Key string
	Value []byte
}

func NewBellyData(b []byte) (*BellyData, error) {
	tweet := new(twitter.Tweet)
	if err := json.Unmarshal(b, tweet); err != nil {
		return nil, err
	}

	ti, err := tweet.CreatedAtTime()
	if err != nil {
		return nil, err
	}
	key := ti.Format("2006-01-02")
	return &BellyData{key, b}, nil
}
