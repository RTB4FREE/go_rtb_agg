package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	cluster "github.com/bsm/sarama-cluster"
)

// BidFields - message format of Kafka bids topic
type BidFields struct {
	ID         string  `json:"oidStr"`
	CampaignID int64   `json:"adid,string"`
	CreativeID int64   `json:"crid,string"`
	AdType     string  `json:"adtype"`
	Domain     string  `json:"domain"`
	Exchange   string  `json:"exchange"`
	Cost       float64 `json:"cost"`
	Timestamp  int64   `json:"timestamp"`
}

// WinFields - message format of Kafka wins topic
type WinFields struct {
	ID         string  `json:"hash"`
	CampaignID int64   `json:"adId,string"`
	CreativeID int64   `json:"cridId,string"`
	AdType     string  `json:"adtype"`
	Exchange   string  `json:"pubId"`
	Cost       string  `json:"cost"` // This can be an empty string or number
	Price      float64 `json:"price,string"`
	Timestamp  int64   `json:"timestamp"`
	Domain     string  `json:"domain"`
}

// PixelFields - message format of Kafka pixels topic
type PixelFields struct {
	ID         string `json:"bid_id"`
	CampaignID int64  `json:"ad_id,string"`
	CreativeID int64  `json:"creative_id,string"`
	AdType     string `json:"adtype"` // Not in msg
	Exchange   string `json:"exchange"`
	Timestamp  int64  `json:"timestamp"`
	Domain     string `json:"domain"`
}

// ClickFields - message format of Kafka clicks topic
type ClickFields struct {
	ID         string `json:"bid_id"`
	CampaignID int64  `json:"ad_id,string"`
	CreativeID int64  `json:"creative_id,string"`
	AdType     string `json:"adtype"` //Not in msg
	Exchange   string `json:"exchange"`
	Timestamp  int64  `json:"timestamp"`
	Domain     string `json:"domain"`
}

// Separate go routine for each topic
func getTopic(config *cluster.Config, brokers []string, topics []string) {
	log1 := logger.GetLogger("getTopic")
	log1.Info("Connect to brokers ", brokers, " topics ", topics)
	consumer, err2 := cluster.NewConsumer(brokers, "rtb-agg-consumer-group", topics, config)
	if err2 != nil {
		panic(err2)
	}
	//defer consumer.Close()
	go func() {
		//log1 := logger.GetLogger("getTopic go func")
		for {
			select {
			case part, ok := <-consumer.Partitions():
				if !ok {
					fmt.Println("consumer.Partions error.")
					return
				}
				// start a separate goroutine to consume messages
				go func(pc cluster.PartitionConsumer) {
					log1 := logger.GetLogger("getTopic kafka partition")
					for msg := range pc.Messages() {
						subCounter.Lock()
						subCounter.count[msg.Topic]++
						subCounter.Unlock()
						log1.Debug(fmt.Sprintf("Kafka subscribe messsage: %s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value))
						switch msg.Topic {
						case "bids":
							fields := BidFields{}
							if err := json.Unmarshal(msg.Value, &fields); err != nil {
								log1.Error(fmt.Sprintf("Kafka subscribe message error: %s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value))
								log1.Error(fmt.Sprintf("JSON unmarshaling bid failed: %s", err))
							} else {
								aggBids.addBid(&fields)
								// Don't do domain agg for bids
							}
						case "wins":
							fields := WinFields{}
							if err := json.Unmarshal(msg.Value, &fields); err != nil {
								log1.Error(fmt.Sprintf("Kafka subscribe message error: %s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value))
								log1.Error(fmt.Sprintf("JSON unmarshaling win failed: %s", err))
							} else {
								aggWins.addWin(&fields, false)
								aggCosts.addCost(&fields, false)
								if enableDomainAggregation {
									aggWinsDom.addWin(&fields, true)
									aggCostsDom.addCost(&fields, true)
								}
							}
						case "pixels":
							fields := PixelFields{}
							if err := json.Unmarshal(msg.Value, &fields); err != nil {
								log1.Error(fmt.Sprintf("Kafka subscribe message error: %s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value))
								log1.Error(fmt.Sprintf("JSON unmarshaling pixel failed: %s", err))
							} else {
								aggPixels.addPixel(&fields, false)
								if enableDomainAggregation {
									aggPixelsDom.addPixel(&fields, true)
								}
							}
						case "clicks":
							fields := ClickFields{}
							if err := json.Unmarshal(msg.Value, &fields); err != nil {
								log1.Error(fmt.Sprintf("Kafka subscribe message error: %s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value))
								log1.Error(fmt.Sprintf("JSON unmarshaling clicks failed: %s", err))
							} else {
								aggClicks.addClick(&fields, false)
								if enableDomainAggregation {
									aggClicksDom.addClick(&fields, true)
								}
							}
						default:
							log1.Alert(fmt.Sprintf("Unexpected topic %s.", msg.Topic))
							return
						}
						consumer.MarkOffset(msg, "") // mark message as processed
					}
				}(part)
			}
		}
	}()
	return
}

// Update counters -
//	Should be concurrency safe since since variable exlusively handled by independent go routines
func (agg OutputCounts) addBid(field *BidFields) {
	var key string
	ts, tsMs, tm := intervalTimestamp(field.Timestamp, intervalSecs)
	keyfields := []string{strconv.FormatInt(field.CampaignID, 10), strconv.FormatInt(field.CreativeID, 10), field.Exchange, intervalStr, ts, field.AdType}
	key = strings.Join(keyfields, "_")
	_, ok := agg[key]
	if !ok {
		agg[key] = CountFields{new(sync.Mutex), 0, tsMs, tm, []string{}}
	}
	agg[key].lock.Lock()
	tmp := agg[key]
	tmp.count++
	if *enableAggArraySend || *debug {
		tmp.ids = append(tmp.ids, field.ID)
	}
	agg[key] = tmp
	agg[key].lock.Unlock()
}
func (agg OutputCounts) addPixel(field *PixelFields, isDomainAgg bool) {
	var key string
	ts, tsMs, tm := intervalTimestamp(field.Timestamp, intervalSecs)
	keyfields := []string{}
	if isDomainAgg {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), intervalStr, ts, field.Domain}
	} else {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), strconv.FormatInt(field.CreativeID, 10), field.Exchange, intervalStr, ts, "unknown"}
	}
	key = strings.Join(keyfields, "_")
	_, ok := agg[key]
	if !ok {
		agg[key] = CountFields{new(sync.Mutex), 0, tsMs, tm, []string{}}
	}
	agg[key].lock.Lock()
	tmp := agg[key]
	tmp.count++
	if *enableAggArraySend || *debug {
		tmp.ids = append(tmp.ids, field.ID)
	}
	agg[key] = tmp
	agg[key].lock.Unlock()
}

func (agg OutputCounts) addClick(field *ClickFields, isDomainAgg bool) {
	var key string
	ts, tsMs, tm := intervalTimestamp(field.Timestamp, intervalSecs)
	keyfields := []string{}
	if isDomainAgg {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), intervalStr, ts, field.Domain}
	} else {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), strconv.FormatInt(field.CreativeID, 10), field.Exchange, intervalStr, ts, "unknown"}
	}

	key = strings.Join(keyfields, "_")
	_, ok := agg[key]
	if !ok {
		agg[key] = CountFields{new(sync.Mutex), 0, tsMs, tm, []string{}}
	}
	agg[key].lock.Lock()
	tmp := agg[key]
	tmp.count++
	if *enableAggArraySend || *debug {
		tmp.ids = append(tmp.ids, field.ID)
	}
	agg[key] = tmp
	agg[key].lock.Unlock()
}

// Wins will increment count and cost sum.
//  So make this a function instead of counter method
func (agg OutputCounts) addWin(field *WinFields, isDomainAgg bool) {
	var key string
	ts, tsMs, tm := intervalTimestamp(field.Timestamp, intervalSecs)
	keyfields := []string{}
	if !isDomainAgg {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), strconv.FormatInt(field.CreativeID, 10), field.Exchange, intervalStr, ts, field.AdType}
	} else {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), intervalStr, ts, field.Domain}
	}
	key = strings.Join(keyfields, "_")
	_, ok := agg[key]
	if !ok {
		agg[key] = CountFields{new(sync.Mutex), 0, tsMs, tm, []string{}}
	}
	agg[key].lock.Lock()
	tmp := agg[key]
	tmp.count++
	if *enableAggArraySend || *debug {
		tmp.ids = append(tmp.ids, field.ID)
	}
	agg[key] = tmp
	agg[key].lock.Unlock()
}

func (agg OutputSums) addCost(field *WinFields, isDomainAgg bool) {
	var key string
	ts, tsMs, tm := intervalTimestamp(field.Timestamp, intervalSecs)
	keyfields := []string{}
	if !isDomainAgg {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), strconv.FormatInt(field.CreativeID, 10), field.Exchange, intervalStr, ts, field.AdType}
	} else {
		keyfields = []string{strconv.FormatInt(field.CampaignID, 10), intervalStr, ts, field.Domain}
	}
	key = strings.Join(keyfields, "_")
	_, ok := agg[key]
	if !ok {
		agg[key] = SumFields{new(sync.Mutex), 0.0, 0.0, 0.0, tsMs, tm}
	}
	agg[key].lock.Lock()
	tmp := agg[key]
	tmp.sumCost += field.Price
	agg[key] = tmp
	agg[key].lock.Unlock()
}

// Compute interval timestamp - string and epoch milliseconds
func intervalTimestamp(timestamp int64, interval int64) (string, int64, time.Time) {
	log1 := logger.GetLogger("intervalTimestamp")
	// Round off to interval
	timestamp = timestamp / 1000 // convert to epoch secs
	timestamp = int64(timestamp/interval) * interval
	tm := time.Unix(int64(timestamp), 0)
	str := tm.UTC().Format(time.RFC3339)
	// Modify date string to match previous agg key format
	//re := regexp.MustCompile(".00Z$")
	//str = re.ReplaceAllString(str, ".000Z")
	epochms := int64(tm.Unix()) * 1000
	log1.Debug(fmt.Sprintf("Interval Time stamp string: %s, %d", str, epochms))
	return str, epochms, tm
}

// Lock counter entry
func (agg OutputCounts) Lock(key string) {
	if _, found := agg[key]; found {
		agg[key].lock.Lock()
	}
}

// Unlock counter entry
func (agg OutputCounts) Unlock(key string) {
	if _, found := agg[key]; found {
		agg[key].lock.Unlock()
	}
}

// Lock sum entry
func (agg OutputSums) Lock(key string) {
	if _, found := agg[key]; found {
		agg[key].lock.Lock()
	}
}

// Unlock sum entry
func (agg OutputSums) Unlock(key string) {
	if _, found := agg[key]; found {
		agg[key].lock.Unlock()
	}
}
