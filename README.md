# go_rtb_agg

This is a Go program that works with the RTB4FREE biddder deployment, as described on http://rtb4free.com. It assumes that you have RTB4FREE bidders logging data to Kafka and an Elasticsearch cluster. Normally the RTB bid, win, pixel, click and cost records are logged individually into Elasticsearch. However this can take lots of disk space on the cluster, especially for long running campaigns.  

This program will subscribe to Kafka and aggregate these counts, and create a log record for 5 minute increments. This will dramatically reduce Elasticsearch disk requirements. The RTB4FREE crosstalk system requires these aggregated logs in order to compute campaign budgets. You can also use the aggregated Elasticsearch index for summary reporting. Here is sample aggregated bid log record.

```
{
    "campaignId": 2,
    "creativeId": 8,
    "adtype": "banner",
    "exchange": "nexage",
    "interval": "5m",
    "region": "US",
    "timestamp": "2019-04-02T23:45:00Z",
    "dbTimestamp": "2019-04-02T23:50:32.606313806Z",
    "bids": 100,
    "wins": 10,
    "pixels": 2,
    "clicks": 1,
    "cost": 1.0,
    "bidIds": null,
    "winIds": null,
    "pixelIds": null,
    "clickIds": null
  }
```
Optionally you can create an aggregated domain record also, as shown here.

```
 {
    "campaignId": 2,
    "domain": "example.com",
    "interval": "5m",
    "clientId": 0,
    "region": "US",
    "timestamp": "2019-04-02T22:55:00Z",
    "dbTimestamp": "2019-04-02T23:00:32.674889232Z",
    "wins": 1,
    "pixels": 0,
    "clicks": 0,
    "cost": 3.5
  }
```




The docker image is available at 

```
ploh/go_rtb_agg_rtb4free.
```

For sample docker start parameters, refer to the docker-compose.xml file. When deployed as a docker swarm, you can define this as a separate stack.The environment variables and sample values are:

* RTBAGG_ESAGGURL: "http://elastic1:9200"
    * URL of ELasticsearch where aggregation records will be created  
* RTBAGG_MYSQLHOST: "db"
    * Hostname of MySQL database where campaign records are stored. 
* RTBAGG_MYSQLUSER: "dbuser"
    * Mysql database user id.
* RTBAGG_MYSQLPASSWORD: "dbpassword"
    * Mysql database user id.
* RTBAGG_ESAGGINDEX: "bidagg".
    * Elasticsearch index name for the aggregated bid records.
* RTBAGG_ENABLEDOMAINAGG: "false"
    * Enable creating domain aggregation records.
* RTBAGG_ESAGGDOMAININDEX: "domainagg"
    * Elasticsearch index name for the aggregated domain records.
* RTBAGG_BROKERLIST: "kafka1:9093"
    * Kafka hostname where bidder logs are published.
* RTBAGG_ENABLEAGGARRAYSEND: "false"
    * Add the ids of the bid, win, pixel and click records to each log record. Only used for test and debugging.



