package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var config = make(map[string]string)

func init() {
	config["cloud1"] = "http://rpulsarless.freeddns.org:32006/function/"
	config["edge1"] = "http://rpulsarless.freeddns.org:32006/function/"
	config["mongo"] = "mongodb://10.8.0.1"
	config["db"] = "rpulsar"
	config["cloud1-prom"] = "http://rpulsarless.freeddns.org:31005/"
	config["cloud2-prom"] = "http://rpulsarless.freeddns.org:31005/"
}

var memory_query = "sum by (pod) (container_memory_working_set_bytes{cluster=\"\",container!=\"\",image!=\"\",job=\"kubelet\",metrics_path=\"/metrics/cadvisor\",namespace=\"openfaas-fn\"})/1000000"
var cpu_query = "sum (rate (container_cpu_usage_seconds_total{image!=\"\", namespace=\"openfaas-fn\"}[1m])) by (pod)"

func connectMongo() *mongo.Client {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(config["mongo"]).SetServerAPIOptions(serverAPI)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")
	return client
}

func insertDocument(collectionName string, doc bson.M, c chan int) {
	var client = connectMongo()
	collection := client.Database(config["db"]).Collection(collectionName)
	res, err := collection.InsertOne(context.Background(), doc)
	err = client.Disconnect(context.TODO())
	if err != nil {
		fmt.Printf("Error inserting document: %v\n", err)
	} else {
		fmt.Printf("Inserted document, id: %v\n", res.InsertedID)
	}
	if err != nil {
		fmt.Printf("Something wrong disconnecting client")
		c <- -1
	} else {
		c <- 1
	}
}
func queryPrometheus(fun string, node string, startTime int64, endTime int64, duration int64, op string) {
	var ep = config[node+"-prom"] + "/api/v1/query_range"
	//var ep = "https://webhook.site/fb5cf153-c9ac-4ca7-9ac7-7cfc94541e62"
	req, _ := http.NewRequest("POST", ep, nil)

	params := req.URL.Query()
	var start = time.Unix(0, startTime).UTC()
	var end = start.Add(time.Duration(int64(time.Second) * duration))
	params.Add("start", start.Format(time.RFC3339))
	params.Add("end", end.Format(time.RFC3339))
	params.Add("step", "1s")
	if op == "memory" {
		params.Add("query", memory_query)
	} else {
		params.Add("query", cpu_query)
	}
	req.URL.RawQuery = params.Encode()
	res, _ := http.DefaultClient.Do(req)
	var resBody, _ = ioutil.ReadAll(res.Body)
	fmt.Printf("Request %v %v\n", ep, params)
	fmt.Printf("%v Received body %s\n", res.StatusCode, resBody)

}

func logMongo(fun string, node string, startTime int64, endTime int64, duration int64) {
	var doc = bson.M{"function": fun, "node": node, "startTime": startTime, "endTime": endTime, "duration": duration}
	c := make(chan int)
	go insertDocument("logs", doc, c)
	if x := <-c; x == -1 {
		fmt.Printf("Something wrong writing log on mongodb")
	} else {
		go queryPrometheus(fun, node, startTime, endTime, duration, "memory")
		go queryPrometheus(fun, node, startTime, endTime, duration, "cpu")
	}

}

func logRequest(node string, fun string, r http.Response) {
	if r.StatusCode >= 200 && r.StatusCode <= 299 {
		var duration, _ = strconv.ParseInt(r.Header.Get("X-Duration-Seconds"), 10, 64)
		var startTime, _ = strconv.ParseInt(r.Header.Get("X-Start-Time"), 10, 64)
		var endTime = startTime + duration

		fmt.Printf("LOG: %v, %v, %v, %v, %v", node, fun, startTime, duration, endTime)
		logMongo(fun, node, startTime, endTime, duration)
	} else {
		fmt.Printf("Status code %v\n", r.StatusCode)
	}
}

func forwardResponse(node string, fun string, headers map[string][]string, body []byte) (*http.Response, error) {
	client := http.Client{}
	var base, ok = config[node]
	if !ok {
		return &http.Response{}, errors.New("Not valid node")
	}
	var url = base + fun
	//var url = "https://www.sci.utah.edu/wrong.html"
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header = headers
	res, err := client.Do(req)
	if err != nil {
		log.Println("Error proxying request")
	} else {
		log.Printf("Forward to %v\n", url)
	}
	logRequest(node, fun, *res)
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
	if err != nil {
		log.Printf("Error %v\n", err)
		c.Status(http.StatusBadRequest)
		return
	}
	var bodyBytes []byte
	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		bodyBytes, _ = io.ReadAll(res.Body)
	} else {
		fmt.Printf("Not 200 response from OpenFaaS: %v\n", res.StatusCode)
	}
	c.Data(res.StatusCode, res.Header.Get("content-type"), bodyBytes)
}

func main() {
	var router = gin.Default()
	router.POST("/proxy", proxy)
	router.Run("localhost:8080")
}
