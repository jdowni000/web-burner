package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/cristoper/gsheet/gdrive"
	"github.com/cristoper/gsheet/gsheets"
)

var json_file_path string
var csv_file_name string
var google_sheet_name string
var google_parent_id string

// Output struct
type PL_JsonStruct struct {
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

type JsonStruct struct {
	Timestamp  string "json:'timestamp'"
	Value      int    "json:'value'"
	UUID       string "json:'uuid'"
	Query      string "json:'query'"
	MetricName string "json:'metricName'"
	JobName    string "json:'jobName'"
}

func init() {

	j := flag.String("j", "", "path to json file")
	c := flag.String("c", "", "csv file name")
	g := flag.String("g", "", "google sheet name")
	p := flag.String("p", "", "google sheet parent id")
	flag.Parse()

	json_file_path = derefString(j)
	csv_file_name = derefString(c)
	google_sheet_name = derefString(g)
	google_parent_id = derefString(p)

	if json_file_path == "" {
		log.Fatal("Please provide path to json file requested using flag '-j'")
	}
	if csv_file_name == "" {
		log.Fatal("Please provide file name for csv file using flag '-c'")
	}
	if google_sheet_name == "" {
		log.Fatal("Please provide file name for the google sheet file using flag '-g'")
	}
	if google_parent_id == "" {
		log.Fatal("Please provide google sheets parent folder id using flag '-p'")
	}
}

func main() {
	err := convert_json_to_csv(json_file_path, csv_file_name)
	if err != nil {
		log.Fatal(err)
	}

	// sheet_id, err := create_google_sheet("test-Friday", "./test-web-burner-8a2dce06046f.json")
	err = write_to_google_sheet(google_sheet_name+".csv", google_parent_id, "./"+csv_file_name)
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

	//Determing which struct to use and unmarshal json
	var pl []PL_JsonStruct
	var js []JsonStruct
	var header []string
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal("Could not find pwd to check for existing files! Aboritng!")
	}

	if strings.Contains(json_file_path, "job-podLatency-summary.json") {
		//unmarshal data
		err = json.Unmarshal([]byte(data), &pl)
		if err != nil {
			return err
		}

		//Delete csv file if it exists and create a new one
		_, err := os.Stat(wd + "/" + destination)
		if err == nil {
			log.Println("CSV filename " + destination + " already exists: Removing file before proceeding!")
			err = os.Remove(destination)
			if err != nil {
				log.Fatal("Failed to remove CSV file with the same name as requested. Terminating to ensure accurate data")
			}
		}
		file, err := os.Create(destination)
		if err != nil {
			return err
		}
		defer file.Close()

		//Write json data to csv file
		header = []string{"quantileName", "uuid", "p99", "p95", "p50", "max", "avg", "timestamp", "metricName", "jobName"}
		w := csv.NewWriter(file)
		err = w.Write(header)
		if err != nil {
			return err
		}

		for _, o := range pl {
			var csvRow []string
			csvRow = append(csvRow, o.QuantileName, o.UUID, strconv.Itoa(o.P99), strconv.Itoa(o.P95), strconv.Itoa(o.P50), strconv.Itoa(o.Max), strconv.Itoa(o.Avg), o.Timestamp, o.MetricName, o.JobName)
			err := w.Write(csvRow)
			if err != nil {
				return err
			}
		}
		w.Flush()
	} else {
		//unmarshal data
		err = json.Unmarshal([]byte(data), &js)
		if err != nil {
			return err
		}

		//Delete csv file if it exists and create a new one
		_, err := os.Stat(wd + "/" + destination)
		if err == nil {
			log.Println("CSV filename " + destination + " already exists: Removing file before proceeding!")
			err = os.Remove(destination)
			if err != nil {
				log.Fatal("Failed to remove CSV file with the same name as requested. Terminating to ensure accurate data")
			}
		}
		file, err := os.Create(destination)
		if err != nil {
			return err
		}
		defer file.Close()

		//Write json data to csv file
		header = []string{"timestamp", "value", "uuid", "query", "metricName", "jobName"}
		w := csv.NewWriter(file)
		err = w.Write(header)
		if err != nil {
			return err
		}

		log.Println("TEST TEST TEST")

		for _, o := range js {
			var csvRow []string
			csvRow = append(csvRow, o.Timestamp, strconv.Itoa(o.Value), o.UUID, o.Query, o.MetricName, o.JobName)
			err := w.Write(csvRow)
			if err != nil {
				return err
			}
		}
		w.Flush()
	}
	return nil
}

func write_to_google_sheet(sheet_name string, parent string, csv_file string) error {

	var r io.Reader

	gdrive_srv, err := gdrive.NewServiceWithCtx(context.TODO())
	if err != nil {
		return err
	}

	new_sheet, err := gdrive_srv.CreateFile(sheet_name, parent, r)
	if err != nil {
		return err
	}

	gsheet_srv, err := gsheets.NewServiceWithCtx(context.TODO())
	if err != nil {
		return err
	}

	r, err = os.Open(csv_file)
	if err != nil {
		return err
	}

	resp, err := gsheet_srv.UpdateRangeCSV(new_sheet.Id, "A001", r)
	if err != nil {
		return err
	}
	log.Println("New google sheet id is:" + resp.SpreadsheetId)
	return nil
}

func derefString(s *string) string {
	if s != nil {
		return *s
	}

	return ""
}
