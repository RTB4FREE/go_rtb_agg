package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"github.com/Shopify/sarama"
	log "github.com/go-ozzo/ozzo-log"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	logk "log"
)

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("kafka:9092").String()
	kafkaVersion        = kingpin.Flag("kafkaVersion", "Kafka version").Default("0.10.2.1").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()

	mysqlHost     = kingpin.Flag("mysqlHost", "MySQL database server host name.").Default("web_db").String()
	mysqlDbname   = kingpin.Flag("mysqlDbname", "MySQL database name.").Default("rtb4free").String()
	mysqlUser     = kingpin.Flag("mysqlUser", "MySQL database user id.").Default("ben").String()
	mysqlPassword = kingpin.Flag("mysqlPassword", "MySQL database password.").Default("test").String()

	esAggURL   = kingpin.Flag("esAggURL", "Elasticsearch aggregation endpoint.").Default("http://192.168.2.102:9200").String()
	esAggIndex = kingpin.Flag("esAggIndex", "Elasticsearch index name for count aggregations. Date stamp will be appended.").Default("agg_index").String()

	enableDomainAgg  = kingpin.Flag("enableDomainAgg", "Enable creation of domain aggregation records.").Bool()
	esAggDomainIndex = kingpin.Flag("esAggDomainIndex", "Elasticsearch index name for domain aggregations. Date stamp will be appended.").Default("agg_domain_index").String()

	debug              = kingpin.Flag("debug", "Output debug messages.").Bool()
	disableAggSend     = kingpin.Flag("disableAggSend", "Disable sending aggregations to Elasticsearch.").Bool()
	enableAggArraySend = kingpin.Flag("enableAggArraySend", "ENable sending IDs for each aggregation in Elasticsearch record.").Bool()
)

// CountFields - stores message count and time interval of message topic
type CountFields struct {
	lock       *sync.Mutex
	count      int
	intervalTs int64
	intervalTm time.Time
	ids        []string
}

// SumFields - stores sum of field value and time interval of message topic (win)
type SumFields struct {
	lock           *sync.Mutex
	sumCost        float64
	sumAdvPrice    float64
	sumAgencyPrice float64
	intervalTs     int64
	intervalTm     time.Time
}

// OutputCounts - Map of unique interval keys to counts
type OutputCounts map[string]CountFields

// OutputSums - Map of unique interval keys to sums
type OutputSums map[string]SumFields

// Output - aggregated record key
//		ie, 1976_3293_openx_5m_2018-03-14T22:55:00.000Z_unknown
const intervalStr string = "5m"
const intervalSecs int64 = 300

// RTB Counters to track
var (
	aggBids   OutputCounts
	aggWins   OutputCounts
	aggCosts  OutputSums
	aggPixels OutputCounts
	aggClicks OutputCounts
)

// Domain Counters to track
var (
	enableDomainAggregation  bool
	disableElasticSearchSend bool
	aggBidsDom               OutputCounts
	aggWinsDom               OutputCounts
	aggCostsDom              OutputSums
	aggPixelsDom             OutputCounts
	aggClicksDom             OutputCounts
)

var logger *log.Logger

// MapCounter - count for each topic.
type MapCounter struct {
	sync.Mutex
	count map[string]int
}

var subCounter = MapCounter{
	count: map[string]int{},
}

func main() {
	//
	logger = log.NewLogger()
	t1 := log.NewConsoleTarget()
	logger.Targets = append(logger.Targets, t1)
	logger.Open()
	log1 := logger.GetLogger("main") // Set the logger "app" field to this functions.
	defer logger.Close()

	//subCounter = make(map[string]int)

	// Set variables from commandline
	kingpin.Parse()

	// Override if environment variable defined - for Docker deploy
	//   env variable format: RTBAGG_<uppercase key>, ie RTBAGG_BROKERLIST
	if v := getEnvValue("brokerList"); v != "" {
		*brokerList = v
	}
	if v := getEnvValue("kafkaVersion"); v != "" {
		*kafkaVersion = v
	}
	if v := getEnvValue("partition"); v != "" {
		*partition = v
	}
	if v := getEnvValue("offsetType"); v != "" {
		val, err := strconv.Atoi(v)
		if err == nil {
			*offsetType = val
		}
	}
	if v := getEnvValue("messageCountStart"); v != "" {
		val, err := strconv.Atoi(v)
		if err == nil {
			*messageCountStart = val
		}
	}
	if v := getEnvValue("mysqlHost"); v != "" {
		*mysqlHost = v
	}
	if v := getEnvValue("mysqlDbname"); v != "" {
		*mysqlDbname = v
	}
	if v := getEnvValue("mysqlUser"); v != "" {
		*mysqlUser = v
	}
	if v := getEnvValue("mysqlPassword"); v != "" {
		*mysqlPassword = v
	}
	if v := getEnvValue("esAggURL"); v != "" {
		*esAggURL = v
	}
	if v := getEnvValue("esAggIndex"); v != "" {
		*esAggIndex = v
	}
	if v := getEnvValue("esAggDomainIndex"); v != "" {
		*esAggDomainIndex = v
	}
	if v := getEnvValue("enableDomainAgg"); v != "" {
		if v == "true" || v == "TRUE" {
			*enableDomainAgg = true
		} else {
			*enableDomainAgg = false
		}
	}
	if v := getEnvValue("debug"); v != "" {
		if v == "true" || v == "TRUE" {
			*debug = true
		} else {
			*debug = false
		}
	}
	if v := getEnvValue("disableAggSend"); v != "" {
		if v == "true" || v == "TRUE" {
			*disableAggSend = true
		} else {
			*disableAggSend = false
		}
	}
	if v := getEnvValue("enableAggArraySend"); v != "" {
		if v == "true" || v == "TRUE" {
			*enableAggArraySend = true
		} else {
			*enableAggArraySend = false
		}
	}

	brokers := strings.Split(*brokerList, ",")
	elasticSearchAggregationURL := *esAggURL
	elasticSearchAggregationIndex := *esAggIndex
	elasticSearchAggregationDomainIndex := *esAggDomainIndex
	enableDomainAggregation = *enableDomainAgg
	if *debug {
		log1.MaxLevel = log.LevelDebug
	} else {
		log1.MaxLevel = log.LevelInfo
	}
	disableElasticSearchSend = *disableAggSend

	now := time.Now().UTC()
	indexDay := fmt.Sprintf("%s-%d.%02d.%02d", elasticSearchAggregationIndex, now.Year(), now.Month(), now.Day())
	log1.Info("Console output level is " + logger.MaxLevel.String())
	log1.Info("Looking for kafka brokers:", brokers)
	log1.Info("Start writing RTB counts to Elasticsearch %s, index %s.", elasticSearchAggregationURL, indexDay)
	log1.Info("Start writing domain counts to Elasticsearch %s, index %s.", elasticSearchAggregationURL, elasticSearchAggregationDomainIndex)
	log1.Info("Read from db host:", *mysqlHost)
	log1.Info(fmt.Sprintf("Enable domain aggregation: %t", enableDomainAggregation))
	log1.Info(fmt.Sprintf("Disable sending aggregation to Elasticsearch: %t", disableElasticSearchSend))

	aggBids = OutputCounts{}
	//aggBids.lock = new(sync.Mutex)
	//aggBids.countFields = map[string]CountFields{}

	aggWins = OutputCounts{}
	aggCosts = OutputSums{}
	aggPixels = OutputCounts{}
	aggClicks = OutputCounts{}

	aggBidsDom = OutputCounts{}
	aggWinsDom = OutputCounts{}
	aggCostsDom = OutputSums{}
	aggPixelsDom = OutputCounts{}
	aggClicksDom = OutputCounts{}
	err := readMySQLTables(*mysqlHost, *mysqlDbname, *mysqlUser, *mysqlPassword)
	if !err {
		for k, v := range dbCampaignRecords {
			logger.Debug("%d Campaign ID %d , Region %s", k, v.ID, v.Regions.String)
		}
		for k, v := range dbBannerRecords {
			logger.Debug("%d Banner %d , width %d, height %d", k, v.ID, v.Width.Int64, v.Height.Int64)
		}
		for k, v := range dbBannerVideoRecords {
			logger.Debug("%d Video %d , width %d, height %d", k, v.ID, v.Width.Int64, v.Height.Int64)
		}
	} else {
		log1.Alert("MySQL error on initial read.") // Let docker restart to reread.  Need initial db to be set.
		panic("MySQL error on initial read.")      // Let docker restart to reread.  Need initial db to be set.
	}

//	config := cluster.NewConfig()
//	config.Group.Mode = cluster.ConsumerModePartitions
	if *debug {
		sarama.Logger = logk.New(os.Stdout, "[sarama] ", logk.LstdFlags)
	}
	version, err2 := sarama.ParseKafkaVersion(*kafkaVersion)
	if err2 != nil {
		panic(err2)
	}
	config := sarama.NewConfig()
	config.Version = version

	// Set up CTL-C to break program
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	//
	//

	// Channel to catch CTL-C
	doneCh := make(chan struct{})
	ticker := time.NewTicker(time.Duration(intervalSecs) * time.Second)

	//Subscribe to Kafka topics
	/*
	go getTopic(config, brokers, []string{"bids"})
	go getTopic(config, brokers, []string{"wins"})
	go getTopic(config, brokers, []string{"pixels"})
	go getTopic(config, brokers, []string{"clicks"})
	*/

	topics := []string{"bids","wins","pixels","clicks"}
	go getTopics(config, brokers, topics)

	// Wait for CTL-C. Will kill all go getTopic routines
	go func() {
		log1 := logger.GetLogger("main go")
		for {
			select {
			case <-signals:
				log1.Alert("Interrupt detected")
				// Drain remaining writes
				allkeys := make(map[string]struct{})
				allkeysDom := make(map[string]struct{})
				var tsMs int64
				setMapKeys(&allkeys, &aggBids, tsMs, true)
				setMapKeys(&allkeys, &aggWins, tsMs, true) // cost keys are included in win keys
				setMapKeys(&allkeys, &aggPixels, tsMs, true)
				setMapKeys(&allkeys, &aggClicks, tsMs, true)
				if enableDomainAggregation {
					setMapKeys(&allkeysDom, &aggBidsDom, tsMs, true)
					setMapKeys(&allkeysDom, &aggWinsDom, tsMs, true) // cost keys are included in win keys
					setMapKeys(&allkeysDom, &aggPixelsDom, tsMs, true)
					setMapKeys(&allkeysDom, &aggClicksDom, tsMs, true)
				}
				writeElastic(elasticSearchAggregationURL, elasticSearchAggregationIndex, elasticSearchAggregationDomainIndex, &allkeys, &allkeysDom)
				log1.Info("Finished sending remaining writes.")
				doneCh <- struct{}{}
			case <-ticker.C:
				fmt.Printf("Ticker at %s\n", time.Now())
				writeLastInterval(elasticSearchAggregationURL, elasticSearchAggregationIndex, elasticSearchAggregationDomainIndex)
			}
		}
	}()
	<-doneCh
	log1.Info("Final unprocessed # records - Bids: %d, Wins: %d, Pixels: %d, Clicks: %d, Cost: %d", len(aggBids), len(aggWins), len(aggCosts), len(aggPixels), len(aggClicks))
	//fmt.Println("Processed status", *messageCountStart, "messages")
	log1.Info("End main")
}

func writeLastInterval(elasticSearchAggregationURL string, elasticSearchAggregationIndex string, elasticSearchAggregationDomainIndex string) {
	log1 := logger.GetLogger("writeLastInterval")
	tsStr, tsMs, _ := intervalTimestamp(int64(int64(time.Now().Unix())*1000), intervalSecs)
	log1.Debug("Interval Time stamp string: %s, %d", tsStr, tsMs)
	allkeys := make(map[string]struct{})
	allkeysDom := make(map[string]struct{})
	tsMs -= intervalSecs * 1000 // Get the previous interval
	setMapKeys(&allkeys, &aggBids, tsMs, false)
	setMapKeys(&allkeys, &aggWins, tsMs, false) // cost keys are included in win keys
	setMapKeys(&allkeys, &aggPixels, tsMs, false)
	setMapKeys(&allkeys, &aggClicks, tsMs, false)
	if enableDomainAggregation {
		setMapKeys(&allkeysDom, &aggBidsDom, tsMs, false)
		setMapKeys(&allkeysDom, &aggWinsDom, tsMs, false) // cost keys are included in win keys
		setMapKeys(&allkeysDom, &aggPixelsDom, tsMs, false)
		setMapKeys(&allkeysDom, &aggClicksDom, tsMs, false)
	}
	err := readMySQLTables(*mysqlHost, *mysqlDbname, *mysqlUser, *mysqlPassword)
	if err {
		log1.Error(fmt.Sprintf("MySQL error. Skip updating campaign db records for cycle %s.", tsStr))
	}

	log1.Debug("All counter keys, before write to elastic:\n")
	log1.Debug("%+v\n",allkeys)


	writeElastic(elasticSearchAggregationURL, elasticSearchAggregationIndex, elasticSearchAggregationDomainIndex, &allkeys, &allkeysDom)
	log1.Info("Unprocessed # records - Bids: %d, Wins: %d, Pixels: %d, Clicks: %d, Cost: %d", len(aggBids), len(aggWins), len(aggCosts), len(aggPixels), len(aggClicks))

	log1.Info("Unprocessed bids:")
	printMapKeys(&aggBids)

	log1.Info("Unprocessed wins:")
	printMapKeys(&aggWins)

	log1.Info("Unprocessed pixels:")
	printMapKeys(&aggPixels)

}

func setMapKeys(set *map[string]struct{}, mymaps *OutputCounts, tsMs int64, sendAll bool) {
	for k, fields := range *mymaps {
		if sendAll || (fields.intervalTs <= tsMs) {
			(*set)[k] = struct{}{} // only set if timestamp >tsMs
		}
	}
	return
}

func printMapKeys(mymaps *OutputCounts) {
	log1 := logger.GetLogger("printMapKeys")
	for k, fields := range *mymaps {
		log1.Info("Key %s  Timestamp %s  Count %d", k, fields.intervalTs, fields.count)
	}
}

func getEnvValue(key string) string {
	log1 := logger.GetLogger("getEnvValue")
	envkey := "RTBAGG_" + strings.ToUpper(string(key))
	if v := os.Getenv(envkey); v != "" {
		log1.Info("Setting %s to env key %s value: %s", key, envkey, v)
		return v
	}
	return ""
}
