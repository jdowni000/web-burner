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

	header := []{"quantileName" string, "uuid" string, "p99" int, "p95" int, "p50" int, "max" int, "avg" int, "timestamp" string, "metricName" string, "jobName" string}
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

func main() {
  err := convert_json_to_csv("collected-metrics/init-served-job-podLatency-summary.json", "output.csv")
  if err != nil {
	log.Fatal(err)
  }
}