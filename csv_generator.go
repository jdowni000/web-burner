package main

import (
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

// Output struct
type Output struct {
	QuantileName string "json:'quantileName'"
	UUID         string "json:'uuid'"
	P99          int    "json:'p99'"
	P95          int    "json:'p95'"
	P50          int    "json:'p50'"
	Max          int    "json:'max'"
	Avg          int    "json:'avg'"
	Timestamp    string "json:'timestamp'"
	MetricName   string "json:'metricName'"
	JobName      string "json:'jobName'"
}

func main() {
	err := convert_json_to_csv("./init-served-job-podLatency-summary.json", "output.csv")
	if err != nil {
		log.Fatal(err)
	}
}

func convert_json_to_csv(source string, destination string) error {

	// Read the JSON file into the struct array
	data, err := ioutil.ReadFile(source)
	if err != nil {
		return err
	}

	//unmarshal data
	var d []Output
	err = json.Unmarshal([]byte(data), &d)
	if err != nil {
		return err
	}

	//create csv file
	f, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer f.Close()

	//Write json data to csv file
	w := csv.NewWriter(f)
	header := []string{"quantileName", "uuid", "p99", "p95", "p50", "max", "avg", "timestamp", "metricName", "jobName"}
	err = w.Write(header)
	if err != nil {
		return err
	}

	for _, o := range d {
		var csvRow []string
		csvRow = append(csvRow, o.QuantileName, o.UUID, strconv.Itoa(o.P99), strconv.Itoa(o.P95), strconv.Itoa(o.P50), strconv.Itoa(o.Max), strconv.Itoa(o.Avg), o.Timestamp, o.MetricName, o.JobName)
		err := w.Write(csvRow)
		if err != nil {
			return err
		}
	}

	// for _, obj := range d {
	// 	var row []string
	// 	row = append(row, obj.QuantileName)
	// 	row = append(row, obj.UUID)
	// 	row = append(row, strconv.Itoa(obj.P99))
	// 	row = append(row, strconv.Itoa(obj.P95))
	// 	row = append(row, strconv.Itoa(obj.P50))
	// 	row = append(row, strconv.Itoa(obj.Avg))
	// 	row = append(row, obj.Timestamp)
	// 	row = append(row, obj.MetricName)
	// 	row = append(row, obj.JobName)
	// 	w.Write(row)
	// }
	w.Flush()
	return nil
}
