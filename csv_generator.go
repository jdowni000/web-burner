package main

import (
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "os"
)

// Output struct
type Output struct {
    quantileName string
    uuid string
    p99 int
    p95 int
	p50 int
	max int
	avg int
	timestamp string
	metricName string
	jobName string
}

func main() {
    // read data from file
    jsonDataFromFile, err := ioutil.ReadFile("collected-metrics/init-served-job-podLatency-summary.json")

    if err != nil {
        fmt.Println(err)
    }

    // Unmarshal JSON data
    var jsonData []Output
    err = json.Unmarshal([]byte(jsonDataFromFile), &jsonData)

    if err != nil {
        fmt.Println(err)
    }

    csvFile, err := os.Create("./output.csv")

    if err != nil {
        fmt.Println(err)
    }
    defer csvFile.Close()

    writer := csv.NewWriter(csvFile)

    for _, usance := range jsonData {
        var row []string
        row = append(row, usance.quantileName)
        row = append(row, usance.uuid)
        row = append(row, usance.p99)
		row = append(row, usance.p95)
		row = append(row, usance.p50)
		row = append(row, usance.max)
		row = append(row, usance.avg)
		row = append(row, usance.timestamp)
		row = append(row, usance.metricName)
		row = append(row, usance.jobName)
        writer.Write(row)
    }

    // remember to flush!
    writer.Flush()
}