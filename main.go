package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PromData struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
				Pod string `json:"pod"`
			} `json:"metric"`
			Values [][]interface{} `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

type Node struct {
	Of   string `json:"of"`
	Prom string `json:"prom"`
}
type Config struct {
	Nodes         map[string]Node `json:"nodes"`
	Mongo         string          `json:"mongo"`
	Db            string          `json:"db"`
	Coll          string          `json:"coll"`
	BufferSize    int             `json:"bufferSize"`
	TimeoutFlush  float64         `json:"timeoutFlush"`
	ListeningHost string          `json:listeningHost`
}

type Buffer struct {
	mu   sync.Mutex
	docs []bson.M
}

var config Config
var mongoBuffer chan bson.M
var lastMongoFetch time.Time
var mongoClient *mongo.Client

func init() {
	configFile, err := os.Open("./config.json")
	if err != nil {
		panic("Error parsing json config file\n")
	}
	byteValue, _ := ioutil.ReadAll(configFile)
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		panic(err)
	}

	mongoBuffer = make(chan bson.M, config.BufferSize)
	lastMongoFetch = time.Now()
	mongoClient = connectMongo()
}

var memoryQuery = "sum by (pod) (container_memory_working_set_bytes{cluster=\"\",container!=\"\",image!=\"\",job=\"kubelet\",metrics_path=\"/metrics/cadvisor\",namespace=\"openfaas-fn\"})/1000000"
var cpuQuery = "sum (rate (container_cpu_usage_seconds_total{image!=\"\", namespace=\"openfaas-fn\"}[1m])) by (pod)"

func connectMongo() *mongo.Client {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(config.Mongo).SetServerAPIOptions(serverAPI)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	//fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")
	return client
}

func getDb() *mongo.Client {
	var result bson.M
	if mongoClient == nil {
		mongoClient = connectMongo()
	} else if err := mongoClient.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		mongoClient = connectMongo()
	}
	return mongoClient
}

func ToSliceOfAny[T any](s []T) []any {
	result := make([]any, len(s))
	for i, v := range s {
		result[i] = v
	}
	return result
}

func dataMonitor() {
	var buffer Buffer
	ticker := time.NewTicker(time.Duration(config.TimeoutFlush * float64(time.Second)))
	go timedFlush(ticker, &buffer)
	for {
		doc := <-mongoBuffer
		buffer.mu.Lock()
		buffer.docs = append(buffer.docs, doc)
		if len(buffer.docs) >= config.BufferSize {
			flushBuffer(&buffer)
		}
		buffer.mu.Unlock()
	}
}

func timedFlush(ticker *time.Ticker, buffer *Buffer) {
	for {
		<-ticker.C
		var timediff = time.Now().Sub(lastMongoFetch)
		var timeout = time.Duration(config.TimeoutFlush * float64(time.Second))
		if timediff > timeout {
			buffer.mu.Lock()
			if len(buffer.docs) > 0 {
				fmt.Println("Automatic buffer flush triggered")
				flushBuffer(buffer)
			}
			buffer.mu.Unlock()
		}

	}
}

func flushBuffer(buffer *Buffer) {
	insertMany(config.Coll, ToSliceOfAny(buffer.docs))
	buffer.docs = nil
	lastMongoFetch = time.Now()
}

func insertMany(collectionName string, docs []interface{}) {
	var client = getDb()
	collection := client.Database(config.Db).Collection(collectionName)
	_, err := collection.InsertMany(context.TODO(), docs)
	if err != nil {
		fmt.Printf("Error during the bulk insert\n")
	} else {
		fmt.Printf("Bulk Insert: %v documents\n", len(docs))
	}
}

func insertDocument(collectionName string, doc bson.M) {
	var client = getDb()
	collection := client.Database(config.Db).Collection(collectionName)
	res, err := collection.InsertOne(context.Background(), doc)
	err = client.Disconnect(context.TODO())
	if err != nil {
		fmt.Printf("Error inserting document: %v\n", err)
	} else {
		fmt.Printf("Inserted document, id: %v\n", res.InsertedID)
	}
	if err != nil {
		fmt.Printf("Something wrong disconnecting client")
	}
}

func parsePromData(jsonString string, fun string) (val [][]interface{}) {
	var data PromData
	json.Unmarshal([]byte(jsonString), &data)
	for _, value := range data.Data.Result {
		var pod = value.Metric.Pod
		if strings.HasPrefix(pod, fun) {
			val = value.Values
			return
		}
	}
	return
}
func queryPrometheus(fun string, node string, startTime time.Time, endTime time.Time, op string) ([][]interface{}, error) {

	var ep = config.Nodes[node].Prom + "/api/v1/query_range"
	req, _ := http.NewRequest("POST", ep, nil)

	params := req.URL.Query()
	var start = startTime.UTC()
	var end = endTime.UTC()
	params.Add("start", start.Format(time.RFC3339))
	params.Add("end", end.Format(time.RFC3339))
	params.Add("step", "1s")
	if op == "memory" {
		params.Add("query", memoryQuery)
	} else {
		params.Add("query", cpuQuery)
	}
	req.URL.RawQuery = params.Encode()
	res, _ := http.DefaultClient.Do(req)
	var resBody, _ = ioutil.ReadAll(res.Body)
	if res.StatusCode == 200 {
		var metric = parsePromData(string(resBody), fun)
		return metric, nil
	}
	return nil, nil
}

func logMongo(fun string, node string, startTime time.Time, endTime time.Time, duration time.Duration, computation time.Duration, params map[string][]string) {
	var mem, cpu [][]interface{}
	var p = make(map[string]string)
	mem, _ = queryPrometheus(fun, node, startTime, endTime, "memory")
	cpu, _ = queryPrometheus(fun, node, startTime, endTime, "cpu")
	for key, values := range params {
		p[key] = values[0]
	}
	var doc = bson.M{"function": fun, "node": node, "startTime": startTime, "endTime": endTime, "duration": duration, "computationTime": computation, "cpu": cpu, "mem": mem, "params": p}
	//insertDocument(DBCOLL, doc)
	//fmt.Printf("Pushed doc to buffer\n")
	mongoBuffer <- doc

}

func logRequest(node string, fun string, r http.Response, params map[string][]string) {
	if r.StatusCode >= 200 && r.StatusCode <= 299 {
		var durationString = r.Header.Get("X-Duration-Seconds")
		var startString = r.Header.Get("X-Start-Time")
		var compDuration = r.Header.Get("X-Computation-Seconds")
		var durNum, _ = strconv.ParseFloat(durationString, 64)
		var startNum, _ = strconv.ParseInt(startString, 10, 64)
		var compNum, _ = strconv.ParseFloat(compDuration, 64)

		var start = time.Unix(0, startNum)
		var duration = time.Duration(float64(time.Second) * durNum)     // nanosecond
		var computation = time.Duration(float64(time.Second) * compNum) // nanosecond

		var end = start.Add(duration)

		fmt.Printf("LOG: %v, %v, %v, %v, %v\n", node, fun, start, end, duration)
		logMongo(fun, node, start, end, duration, computation, params)
	} else {
		fmt.Printf("Status code %v\n", r.StatusCode)
	}

}

func forwardResponse(node string, fun string, headers map[string][]string, body []byte) (*http.Response, error) {
	client := http.Client{}
	var base = config.Nodes[node].Of
	var url = base + fun
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header = headers
	res, err := client.Do(req)
	if err != nil {
		log.Println("Error proxying request")
	} else {
		log.Printf("Forward to %v\n", url)
	}
	return res, err
}

func proxy(c *gin.Context) {
	var fun = c.Request.FormValue("function")
	var node = c.Request.FormValue("node")
	if fun == "" || node == "" {
		c.Status(http.StatusBadRequest)
		return
	}
	var header = c.Request.Header
	var body, err = io.ReadAll(c.Request.Body)
	var contenttype = c.Request.Header.Get("content-type")
	if err != nil {
		fmt.Printf("Error %v\n", err)
	}
	//fmt.Printf("Function %v, header: %v, Body: %v", fun, header, body)
	var fheaders = make(map[string][]string)
	var prefix = "X-Config"
	for key, value := range header {
		// forwarding config
		if strings.HasPrefix(key, prefix) {
			fheaders[strings.Replace(key, prefix, "", 1)] = value
		}
	}
	fheaders["content-type"] = []string{contenttype}
	res, err := forwardResponse(node, fun, fheaders, body)
	paramPairs := c.Request.URL.Query()
	go logRequest(node, fun, *res, paramPairs)
	if err != nil {
		log.Printf("Error %v\n", err)
		c.Status(http.StatusBadRequest)
		return
	}
	var bodyBytes, _ = io.ReadAll(res.Body)
	if !(res.StatusCode >= 200 && res.StatusCode <= 299) {
		fmt.Printf("Error %v from OpenFaaS: %v\n", res.StatusCode, string(bodyBytes))
	}
	c.Data(res.StatusCode, res.Header.Get("content-type"), bodyBytes)
}

func main() {
	var router = gin.Default()
	go dataMonitor()
	router.POST("/proxy", proxy)
	router.Run(config.ListeningHost)
	_ = 1
}
