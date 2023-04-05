package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var mem = ` {"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"env-bb9765949-77t7k"},"values":[[1680706739,"0"]]},{"metric":{"pod":"printer-6dd8ff9bfc-tpz47"},"values":[[1680706739,"0.0003876663285227863"]]},{"metric":{"pod":"random-text-64494fc59c-qzh22"},"values":[[1680706739,"0.0006205269973804884"]]},{"metric":{"pod":"shasum-5c9c4d9645-5cf5g"},"values":[[1680706739,"0.0005602004439362469"]]},{"metric":{"pod":"transfer-rand-obj-86c54f56cb-72qph"},"values":[[1680706739,"0.0007308966389995621"]]},{"metric":{"pod":"write-on-mongo-6cb6d8ff48-vhdjr"},"values":[[1680706739,"0"]]}]}}`
var cpu = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"cows-74b8bd9675-wl6hd"},"values":[[1680708154,"19.103744"]]},{"metric":{"pod":"curl-644d87d8f7-lmdrj"},"values":[[1680708154,"3.780608"]]},{"metric":{"pod":"detect-fire-747fcccfbf-6jpbp"},"values":[[1680708154,"6.447104"]]},{"metric":{"pod":"env-bb9765949-77t7k"},"values":[[1680708154,"3.780608"]]},{"metric":{"pod":"extract-word-64b566f944-cbncq"},"values":[[1680708154,"5.640192"]]},{"metric":{"pod":"figlet-8d8fdb95f-j48nl"},"values":[[1680708154,"3.833856"]]},{"metric":{"pod":"hash-68868cc476-m5p7n"},"values":[[1680708154,"5.500928"]]},{"metric":{"pod":"obj-ocr-74bbd85769-b9mlk"},"values":[[1680708154,"5.7344"]]},{"metric":{"pod":"printer-6dd8ff9bfc-tpz47"},"values":[[1680708154,"7.028736"]]},{"metric":{"pod":"random-text-64494fc59c-qzh22"},"values":[[1680708154,"5.869568"]]},{"metric":{"pod":"shasum-5c9c4d9645-5cf5g"},"values":[[1680708154,"3.616768"]]},{"metric":{"pod":"transfer-rand-obj-86c54f56cb-72qph"},"values":[[1680708154,"5.869568"]]},{"metric":{"pod":"write-on-mongo-6cb6d8ff48-vhdjr"},"values":[[1680708154,"7.602176"]]}]}}`

type T struct {
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

func getMetric(data *T, jsonString string, fun string) (val [][]interface{}) {
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
func getData() {
	var data T
	var fun = "cows"
	var cpu_metrics, mem_metrics [][]interface{}
	cpu_metrics = getMetric(&data, cpu, fun)
	mem_metrics = getMetric(&data, mem, fun)
	fmt.Printf("CPU %v: %v\n", fun, cpu_metrics)
	fmt.Printf("MEM %v: %v\n", fun, mem_metrics)

}

func main() {
	var start_str = "1680713081910870799"
	var dur_str = "0.002503"
	var start_num, _ = strconv.ParseInt(start_str, 10, 64)
	var dur_num, _ = strconv.ParseFloat(dur_str, 64)

	var start_time = time.Unix(0, start_num)
	var dur_time = time.Duration(float64(time.Second) * dur_num)
	var end_time = start_time.Add(dur_time)
	fmt.Printf("%v == %v\n%v\n", start_time, end_time, dur_time)

}
