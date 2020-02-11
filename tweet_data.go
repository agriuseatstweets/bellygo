package main

import (
	"encoding/json"
    "github.com/dghubble/go-twitter/twitter"
)

type UBOriginal struct {
	ID int64 `json:"id,omitempty"`
	Userid int64 `json:"userID,omitempty"`
	Text string `json:"text,omitempty"`
    Source string `json:"source,omitempty"`
    Idate string `json:"iDate,omitempty"`
    Truncated int `json:"truncated,omitempty"`
    Retweetid int64 `json:"retweetID,omitempty"`
    Retweetuserid int64 `json:"retweetUserID,omitempty"`
    Retweet int `json:"retweet,omitempty"`
    Sensitive int `json:"sensitive,omitempty"`
    Lang string `json:"lang,omitempty"`
    Created string `json:"created,omitempty"`
    Lat float64 `json:"lat,omitempty"`
    Lng float64 `json:"lng,omitempty"`
    Placeid string `json:"placeID,omitempty"`
    Placetype string `json:"placeType,omitempty"`
    Placename string `json:"placeName,omitempty"`
    Hashtags string `json:"hashtags,omitempty"`
    Mentions string `json:"mentions,omitempty"`
    Urls string `json:"urls,omitempty"`
    Symbols string `json:"symbols,omitempty"`
    Ufollowers int `json:"ufollowers,omitempty"`
    Ufriends int `json:"ufriends,omitempty"`
    Ulisted int `json:"ulisted,omitempty"`
    Ufavourites int `json:"ufavourites,omitempty"`
    Utweets int `json:"utweets,omitempty"`
    Udefaultprofile int `json:"udefaultProfile,omitempty"`
    Udefaultprofileimage int `json:"udefaultProfileImage,omitempty"`
    Placecountry string `json:"placeCountry,omitempty"`
    Contributoruserid int64 `json:"ContributorUserID,omitempty"`
    Isquotestatus int `json:"isQuoteStatus,omitempty"`
    Numtimesretweeted int `json:"numTimesRetweeted,omitempty"`
    Numlikes int `json:"numLikes,omitempty"`
    Favorite int `json:"favorite,omitempty"`
    Filter string `json:"filter,omitempty"`
    Month int `json:"month,omitempty"`
}

type RehydratedTweet struct {
	*twitter.Tweet
	TH_rehydrated bool `json:"th_rehydrated,omitempty"`
	TH_original *UBOriginal `json:"th_original,omitempty"`
}

type TweetData struct {
	Tweet *RehydratedTweet
	Bytes []byte
}


func NewTweetData(val []byte) (*TweetData, error) {
	dat := new(RehydratedTweet)
	if err := json.Unmarshal(val, dat); err != nil {
		return nil, err
	}
	return &TweetData{dat, val}, nil
}
