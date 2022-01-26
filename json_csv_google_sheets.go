package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"io"
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
var reader io.Reader

// Struct specifically for Pod Latency summary json files
type PodLatencyStruct struct {
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

// Default strcut to use for json files
type JsonStruct struct {
	Timestamp  string "json:'timestamp'"
	Labels     Labels "json:'labels'"
	Value      int    "json:'value'"
	UUID       string "json:'uuid'"
	Query      string "json:'query'"
	MetricName string "json:'metricName'"
	JobName    string "json:'jobName'"
}

//struct for nested object in JsonStruct
type Labels struct {
	Node string "json:'instance'"
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

	//Determine present working directory
	wd, err := os.Getwd()
	error_check(err)

	//create csv file
	csv_file, err := create_csv(wd, csv_file_name)
	error_check(err)

	//create google sheet
	sheed_id, err := create_gs(google_sheet_name, google_parent_id)
	error_check(err)

	//unmarshal json into csv file
	struct_req := json_identifier(json_file_path)

	//upload csv to google sheets

}

//func create_csv creates a csv file
func create_csv(wd string, csv_file_name string) (*os.File, error) {
	var empty *os.File
	csv_file_name = csv_file_name + ".csv"
	//Delete csv file if it exists
	_, err := os.Stat(wd + "/" + csv_file_name)
	if err == nil {
		log.Println("CSV filename " + csv_file_name + " already exists: Removing existing file before proceeding!")
		err = os.Remove(csv_file_name)
		if err != nil {
			return empty, err
		}
	}
	//create csv file
	file, err := os.Create(csv_file_name)
	if err != nil {
		return empty, err
	}
	defer file.Close()
	return file, nil
}

//func json_to_csv takes a json file and unmarshalls it to a defined csv file
func json_to_csv(json_file string, struct_req string, csv_file *os.File) error {

	w := csv.NewWriter(csv_file)

	if struct_req == "pod_latency_struct" {
		var pl []PodLatencyStruct
		//unmarshal data
		err := json.Unmarshal([]byte(json_file), &pl)
		if err != nil {
		}
		//header := []string{"timestamp", "value", "uuid", "query", "metricName", "jobName"}
		// Clear up logic exactly what Murali wants here for csv file format
		//code....s
		//err = w.Write(header)

	}
	var js []JsonStruct
	//unmarshal data
	err := json.Unmarshal([]byte(json_file), &js)
	if err != nil {
		return err
	}
	return nil
}

//
func data_calc(j []JsonStruct) {
	var job_name []string
	var node []string
	var csv_row []string
	max := 0

	for _, jn := range j {
		if !exists(job_name, jn.JobName) {
			job_name = append(job_name, jn.JobName)
		}
	}

	for _, n := range j {
		if !exists(node, n.Labels.Node) {
			node = append(node, n.Labels.Node)
		}
	}

	for _, c := range j {
		if !exists(job_name, c.JobName) || !exists(node, c.Labels.Node) {
			csv_row = append(csv_row, c.JobName, c.Labels.Node, //cvalue max)
		}
	}

	for _, value := range j {
		if value.Value > max {
			max = value.Value
			csv_row = append(csv_row, value.JobName, value.Labels.Node, strconv.Itoa(max))

		}
	}

}

//func exists checks if an element exists againnt an array
func exists(a []string, element string) bool {
	for _, e := range a {
		if e == element {
			return true
		}
	}
	return false
}

//func json_identifier determines what json file is being used to pass in correct struct for unmarsahlling
func json_identifier(json_file string) string {

	var json_struct_req string

	if strings.Contains(json_file, "job-podLatency-summary.json") {
		json_struct_req = "pod_latency_struct"
		return json_struct_req
	}
	json_struct_req = "json_struct"
	return json_struct_req
}

//func create_gs creates a new google spreadsheet
func create_gs(sheet_name string, parent string) (string, error) {
	var r io.Reader

	gdrive_srv, err := gdrive.NewServiceWithCtx(context.TODO())
	if err != nil {
		return "", err
	}

	new_sheet, err := gdrive_srv.CreateFile(sheet_name, parent, r)
	if err != nil {
		return "", err
	}

	return new_sheet.Id, nil
}

//func write_to_google_sheets creates a specified google sheet utilizing an existing csv file
func write_to_google_sheet(csv_file string, sheet_name string, parent string, sheet_id string) error {

	gsheet_srv, err := gsheets.NewServiceWithCtx(context.TODO())
	if err != nil {
		return err
	}

	r, err := os.Open(csv_file)
	if err != nil {
		return err
	}

	resp, err := gsheet_srv.UpdateRangeCSV(sheet_id, "A001", r)
	if err != nil {
		return err
	}
	log.Println("New google sheet id is:" + resp.SpreadsheetId)
	return nil
}

//func derefString removes the pointer to a string
func derefString(s *string) string {
	if s != nil {
		return *s
	}

	return ""
}

//func error checks error and exits if is not empty
func error_check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
