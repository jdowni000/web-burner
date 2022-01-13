package main

import (
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io/ioutil"
	"log"
    "os"
)

// Output struct
type Output struct {
    QuantileName string 'json:"quantileName"'
    UUID string 'json:"uuid"'
    P99 int  'json:"p99"'
    P95 int  'json:"p95"'
	P50 int  'json:"p50"'
	Max int  'json:"max"'
	Avg int  'json:"avg"'
	Timestamp string  'json:"timestamp"'
	MetricName string  'json:"metricName"'
	JobName string  'json:"jobName"'
}

func convert_json_to_csv(source string, destination string) error {
    
	// Read the JSON file into the struct array
	source_file, err := os.Open(source)
	if err != nil {
		return err
	}

	defer source_file.Close()

	var json_data []Output
	err := json.NewDecoder(source_file).Decode(&json_data)
	if err != nil {
		return err
	}
    
	// Create new file
	output_file, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer output_file.Close()

	// Write the header of the CSV file
	write := csv.NewWriter(output_file)
	defer writer.Flush()

	header := []string{"quantileName", "uuid", "p99", "p95", "p50", "max", "avg", "timestamp", "metricName", "jobName"}
    err := writer.Write(header)	
	if err != nil {
		return err
	}

	for _, o := range json_data {
		var csvRow []string
		csvRow = append(csvRow, o.QuantileName, o.UUID, o.P99, o.P95, o.P50, o.Max, o.Avg, o.Timestamp, o.MetricName, o.JobName)
		err := writer.Write(csvRow)
		if err != nil {
			return err
		}
	}
	return nil
}

func json_to_csv(json_file string) {
    // read data from file
    // jsonDataFromFile, err := ioutil.ReadFile("collected-metrics/init-served-job-podLatency-summary.json")
    jsonDataFromFile, err := ioutil.ReadFile(json_file)
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

func main() {
  if err := convert_json_to_csv("collected-metrics/init-served-job-podLatency-summary.json", "output.csv")
    log.Fatal(err)
}