package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	neturl "net/url"
	//"github.com/olivere/elastic"

	// MUST USE COMPATIBLE ELASTICSEARCH VERSION
	"gopkg.in/olivere/elastic.v6"
)

// AdSize - embedded structure for AggCounter Elasticsearch
type AdSize struct {
	AdType          string `json:"adtype"`
	ExtCreativeID   int64  `json:"ext_creative_id"`
	WidthHeightList string `json:"width_height_list"`
}

// AggCounter - Elasticsearch RTB counts aggregation record format, used in send/index to Elasticsearch
type AggCounter struct {
	CampaignID  int64     `json:"campaignId"`
	CreativeID  int64     `json:"creativeId"`
	AdSize      AdSize    `json:"adsize"`
	AdType      string    `json:"adtype"`
	Exchange    string    `json:"exchange"`
	Interval    string    `json:"interval"`
	Region      string    `json:"region"`
	Timestamp   time.Time `json:"timestamp"`
	DbTimestamp time.Time `json:"dbTimestamp"`
	Bids        int       `json:"bids"`
	Wins        int       `json:"wins"`
	Pixels      int       `json:"pixels"`
	Clicks      int       `json:"clicks"`
	Cost        float64   `json:"cost"`
	BidIDs      []string  `json:"bidIds"`
	WinIDs      []string  `json:"winIds"`
	PixelIDs    []string  `json:"pixelIds"`
	ClickIDs    []string  `json:"clickIds"`
}

// DomainCounter - Elasticsearch domain aggregation record format, used in send/index to Elasticsearch
type DomainCounter struct {
	CampaignID  int64     `json:"campaignId"`
	Domain      string    `json:"domain"`
	Interval    string    `json:"interval"`
	ClientID    int64     `json:"clientId"`
	Region      string    `json:"region"`
	Timestamp   time.Time `json:"timestamp"`
	DbTimestamp time.Time `json:"dbTimestamp"`
	Wins        int       `json:"wins"`
	Pixels      int       `json:"pixels"`
	Clicks      int       `json:"clicks"`
	Cost        float64   `json:"cost"`
}

func writeElastic(elasticSearchAggregationURL string, elasticSearchAggregationIndex string, elasticSearchAggregationDomainIndex string,
	allkeys *map[string]struct{}, allkeysDom *map[string]struct{}) {

	log1 := logger.GetLogger("writeElastic")
	client, err := elastic.NewSimpleClient(elastic.SetURL(elasticSearchAggregationURL)) // US
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer client.Stop()
	ctx := context.Background()
	for k := range *allkeys {
		aggBids.Lock(k)
		aggWins.Lock(k)
		aggClicks.Lock(k)
		aggPixels.Lock(k)
		aggCosts.Lock(k)
		log1.Info(fmt.Sprintf("Writing entry key %s.", k))
		s := strings.Split(k, "_")
		campaignID, _ := strconv.ParseInt(s[0], 10, 64)
		creativeID, _ := strconv.ParseInt(s[1], 10, 64)
		exchange := s[2]
		intervalStr := s[3]
		//intervalTs := s[4]
		adType := s[5]
		campaignRec := CampaignFields{}
		campaignRec = dbCampaignRecords.findID(campaignID)
		found := false
		var widthHeightList string
		var extCreativeID int64
		if adType != "video" { // Confirm or check this is a  banner. If unknown, check if banner first.
			_, found = dbBannerRecords.findID(campaignID, creativeID)
			if found {
				adType = "banner"
				log1.Info("Found banner.")
			}
		}
		if !found { // Not found or adtype is video
			_, found = dbBannerVideoRecords.findID(campaignID, creativeID)
			if !found && adType == "video" { // Check condition if a banner was mislabeled as video (not found in videos db)
				log1.Debug("Not Video. Checking if banner.")
				_, found = dbBannerRecords.findID(campaignID, creativeID)
				if found {
					log1.Debug("Found as banner.")
					adType = "banner"
				} else {
					// log this with campId and creatID
					log1.Debug("Not Found in banners. Set to Unknown.")
					adType = "unknown"
				}
			} else {
				log1.Debug("adType is video.")
			}
		}

		var intervalTime time.Time
		if aggBids[k].count > 0 {
			intervalTime = aggBids[k].intervalTm
		} else if aggWins[k].count > 0 {
			intervalTime = aggWins[k].intervalTm
		} else if aggPixels[k].count > 0 {
			intervalTime = aggPixels[k].intervalTm
		} else if aggClicks[k].count > 0 {
			intervalTime = aggClicks[k].intervalTm
		} else {
			intervalTime = time.Now()
		}

		adsize := AdSize{
			AdType:          adType,
			ExtCreativeID:   extCreativeID,
			WidthHeightList: widthHeightList,
		}

		// Index a elasticsearch record (using JSON serialization)
		now := time.Now().UTC()
		aggrec := AggCounter{
			CampaignID:  campaignID,
			CreativeID:  creativeID,
			AdType:      adType,
			AdSize:      adsize,
			Exchange:    exchange,
			Interval:    intervalStr,
			Region:      campaignRec.Regions.String,
			Timestamp:   intervalTime,
			DbTimestamp: now,
			Bids:        aggBids[k].count,
			Wins:        aggWins[k].count,
			Pixels:      aggPixels[k].count,
			Clicks:      aggClicks[k].count,
			Cost:        aggCosts[k].sumCost,
		}
		if *enableAggArraySend || *debug {
			aggrec.BidIDs = aggBids[k].ids
			aggrec.WinIDs = aggWins[k].ids
			aggrec.PixelIDs = aggPixels[k].ids
			aggrec.ClickIDs = aggClicks[k].ids
		}
		indexDay := fmt.Sprintf("%s-%d.%02d.%02d", elasticSearchAggregationIndex, now.Year(), now.Month(), now.Day())
		recID := strings.Join(s[0:5], "_") + "_" + adType + "_" + fmt.Sprintf("%d", now.UnixNano())
		log1.Info(fmt.Sprintf("Agg record: %v.", aggrec))
		if !disableElasticSearchSend {
			put1, err := client.Index().
				Index(indexDay).
				Type("campaign_perf").
				Id(recID).
				BodyJson(aggrec).
				Do(ctx)
			if err != nil {
				// Handle error
				fmt.Println(err.Error())
				continue
			}
			log1.Info(fmt.Sprintf("Indexed agg record %s to index %s, %v.", put1.Id, put1.Index, aggrec))
		} else {
			log1.Info(fmt.Sprintf("Indexed agg record %s to index %s, %v.", recID, indexDay, aggrec))
		}
		// variables should be concurrency safe since date stamped keys delete old keys
		delete(aggBids, k)
		delete(aggWins, k)
		delete(aggPixels, k)
		delete(aggClicks, k)
		delete(aggCosts, k)
		aggBids.Unlock(k)
		aggWins.Unlock(k)
		aggClicks.Unlock(k)
		aggPixels.Unlock(k)
		aggCosts.Unlock(k)
	}

	////////////////
	for k := range *allkeysDom {
		aggWinsDom.Lock(k)
		aggClicksDom.Lock(k)
		aggPixelsDom.Lock(k)
		aggCostsDom.Lock(k)

		log1.Info(fmt.Sprintf("Writing domain entry key %sn", k))
		s := strings.Split(k, "_")
		campaignID, _ := strconv.ParseInt(s[0], 10, 64)
		intervalStr := s[1]
		//intervalTs := s[2]
		domain := strings.Join(s[3:], "_") // domain can contain "_". If so, will be re-joined
		domain, _ = neturl.PathUnescape(domain)
		campaignRec := CampaignFields{}
		campaignRec = dbCampaignRecords.findID(campaignID)
		// Set time slice interval
		var intervalTime time.Time
		if aggWinsDom[k].count > 0 {
			intervalTime = aggWinsDom[k].intervalTm
		} else if aggPixelsDom[k].count > 0 {
			intervalTime = aggPixelsDom[k].intervalTm
		} else if aggClicksDom[k].count > 0 {
			intervalTime = aggClicksDom[k].intervalTm
		} else {
			intervalTime = time.Now()
		}

		// Index a elasticsearch record (using JSON serialization)
		now := time.Now().UTC()
		//extLineId, _ := strconv.ParseInt(campaignRec.ExtLineID, 10, 64)
		aggrec := DomainCounter{
			CampaignID:  campaignID,
			Domain:      domain,
			Interval:    intervalStr,
			Region:      campaignRec.Regions.String,
			Timestamp:   intervalTime,
			DbTimestamp: now,
			Wins:        aggWinsDom[k].count,
			Pixels:      aggPixelsDom[k].count,
			Clicks:      aggClicksDom[k].count,
			Cost:        aggCostsDom[k].sumCost,
		}
		indexDay := fmt.Sprintf("%s-%d.%02d.%02d", elasticSearchAggregationDomainIndex, now.Year(), now.Month(), now.Day())
		if !disableElasticSearchSend {
			put1, err := client.Index().
				Index(indexDay).
				Type("campaign_domain").
				Id(k).
				BodyJson(aggrec).
				Do(ctx)
			if err != nil {
				// Handle error
				fmt.Println(err.Error())
				fmt.Printf("ES structure %v.", aggrec)
				continue
			}
			log1.Info(fmt.Sprintf("Indexed domain agg record %s to index %s, %v.", put1.Id, put1.Index, aggrec))
		} else {
			log1.Info(fmt.Sprintf("Indexed domain agg record %s to index %s, %v.", k, indexDay, aggrec))
		}
		// variables should be concurrency safe since date stamped keys delete old keys
		delete(aggWinsDom, k)
		delete(aggPixelsDom, k)
		delete(aggClicksDom, k)
		delete(aggCostsDom, k)
		aggWinsDom.Unlock(k)
		aggClicksDom.Unlock(k)
		aggPixelsDom.Unlock(k)
		aggCostsDom.Unlock(k)
	}
	subCounter.Lock()
	for k, v := range subCounter.count {
		log1.Info(fmt.Sprintf("Number of subscribe messages for %s: %d", k, v))
		subCounter.count[k] = 0
	}
	//subCounter = make(map[string]int)
	subCounter.Unlock()
	return
}
