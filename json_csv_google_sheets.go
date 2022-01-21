package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/cristoper/gsheet/gdrive"
	"github.com/cristoper/gsheet/gsheets"
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

	// sheet_id, err := create_google_sheet("test-Friday", "./test-web-burner-8a2dce06046f.json")
	err = write_to_google_sheet("test_friday.csv", "1OQsqNu96iZ2DBBcJBJdFFzYyUahTsZbF", "./output.csv")
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
	w.Flush()
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
	log.Println(new_sheet.Id)

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
	log.Println(resp)
	return nil
}
