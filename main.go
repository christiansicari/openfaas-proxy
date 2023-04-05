package main

import (
	"bytes"
	"context"
	"encoding/json"
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

var config = make(map[string]string)

func init() {
	config["cloud1"] = "http://rpulsarless.freeddns.org:32006/function/"
	config["edge1"] = "http://rpulsarless.freeddns.org:32006/function/"
	config["mongo"] = "mongodb://10.8.0.1"
	config["db"] = "rpulsar"
	config["cloud1-prom"] = "http://rpulsarless.freeddns.org:31005/"
	config["cloud2-prom"] = "http://rpulsarless.freeddns.org:31005/"
}

var memoryQuery = "sum by (pod) (container_memory_working_set_bytes{cluster=\"\",container!=\"\",image!=\"\",job=\"kubelet\",metrics_path=\"/metrics/cadvisor\",namespace=\"openfaas-fn\"})/1000000"
var cpuQuery = "sum (rate (container_cpu_usage_seconds_total{image!=\"\", namespace=\"openfaas-fn\"}[1m])) by (pod)"

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

func insertDocument(collectionName string, doc bson.M) {
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
	var ep = config[node+"-prom"] + "/api/v1/query_range"
	//var ep = "https://webhook.site/fb5cf153-c9ac-4ca7-9ac7-7cfc94541e62"
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

func logMongo(fun string, node string, startTime time.Time, endTime time.Time, duration time.Duration) {
	var mem, cpu [][]interface{}

	mem, _ = queryPrometheus(fun, node, startTime, endTime, "memory")
	cpu, _ = queryPrometheus(fun, node, startTime, endTime, "cpu")
	var doc = bson.M{"function": fun, "node": node, "startTime": startTime, "endTime": endTime, "duration": duration, "cpu": cpu, "mem": mem}
	insertDocument("logs", doc)

}

func logRequest(node string, fun string, r http.Response) {
	if r.StatusCode >= 200 && r.StatusCode <= 299 {

		var durationString = r.Header.Get("X-Duration-Seconds")
		var startString = r.Header.Get("X-Start-Time")
		var durNum, _ = strconv.ParseFloat(durationString, 64)
		var startNum, _ = strconv.ParseInt(startString, 10, 64)

		var start = time.Unix(0, startNum)
		var duration = time.Duration(float64(time.Second) * durNum)
		var end = start.Add(duration)

		fmt.Printf("LOG: %v, %v, %v, %v, %v\n", node, fun, start, end, duration)
		logMongo(fun, node, start, end, duration)
	} else {
		fmt.Printf("Status code %v\n", r.StatusCode)
	}
}

func forwardResponse(node string, fun string, headers map[string][]string, body []byte) (*http.Response, error) {
	client := http.Client{}
	var base, ok = config[node]
	if !ok {
		return &http.Response{}, errors.New("not valid node")
	}
	var url = base + fun
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header = headers
	res, err := client.Do(req)
	if err != nil {
		log.Println("Error proxying request")
	} else {
		log.Printf("Forward to %v\n", url)
	}
	go logRequest(node, fun, *res)
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
