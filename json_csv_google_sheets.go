package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/cristoper/gsheet/gdrive"
	"github.com/cristoper/gsheet/gsheets"
	"google.golang.org/api/sheets/v4"
)

var json_file_path string
var csv_file_name string
var google_sheet_file_name string
var google_parent_id string
var new_gs_req bool
var existing_sheet_id string

// Struct for Pod Latency summary json files
type PodLatencyStruct struct {
	QuantileName string `json:"quantileName"`
	UUID         string `json:"uid"`
	P99          int    `json:"p99"`
	P95          int    `json:"p95"`
	P50          int    `json:"p50"`
	Max          int    `json:"max"`
	Avg          int    `json:"avg"`
	Timestamp    string `json:"timestamp"`
	MetricName   string `json:"metricName"`
	JobName      string `json:"jobName"`
}

// Default strcut to use for json files
type JsonStructValInt struct {
	Timestamp string `json:"timestamp"`
	Labels    struct {
		Node string `json:"instance"`
	} `json:"labels"`
	Value      int    `json:"value"`
	UUID       string `json:"uuid"`
	Query      string `json:"query"`
	MetricName string `json:"metricName"`
	JobName    string `json:"jobName"`
}

// Struct for Json files that have a value that is a float and a label.instance key
type JsonStructValFloatInstance struct {
	Timestamp string `json:"timestamp"`
	Labels    struct {
		Node string `json:"instance"`
	} `json:"labels"`
	Value      float64 `json:"value"`
	UUID       string  `json:"uuid"`
	Query      string  `json:"query"`
	MetricName string  `json:"metricName"`
	JobName    string  `json:"jobName"`
}

// Struct for Json files that have a value that is a float and a label.node key
type JsonStructValFloatNode struct {
	Timestamp string `json:"timestamp"`
	Labels    struct {
		Node string `json:"node"`
	} `json:"labels"`
	Value      float64 `json:"value"`
	UUID       string  `json:"uuid"`
	Query      string  `json:"query"`
	MetricName string  `json:"metricName"`
	JobName    string  `json:"jobName"`
}

func init() {

	j := flag.String("json", "", "path to json file")
	c := flag.String("csv", "output.csv", "csv file name")
	g := flag.String("gs", "web-burner output", "google sheet name")
	p := flag.String("parent", "", "google sheet parent id")
	n := flag.Bool("new", true, "This bool request for a new google sheet to be made")
	s := flag.String("sheetid", "", "Google Sheet ID if new google sheet is not requested using flag '-n' and has been set to false")
	flag.Parse()

	json_file_path = derefString(j)
	csv_file_name = derefString(c)
	google_sheet_file_name = derefString(g)
	google_parent_id = derefString(p)
	existing_sheet_id = derefString(s)
	new_gs_req = *n

	if json_file_path == "" {
		log.Fatal("Please provide path to json file requested using flag '-j'")
	}
	if csv_file_name == "" {
		log.Println("No file name provided for the csv file using flag '-c', using default value output.csv")

	}
	if !(check_suffix(csv_file_name)) {
		log.Println("No csv filename extenstion found in provided csv filename, adding extension to requested csv filename")
		csv_file_name = csv_file_name + ".csv"
	}
	if google_sheet_file_name == "" {
		log.Println("No name for google sheet provided using '-g', providing default google sheet 'web-burner output'")
	}
	if !(check_suffix(google_sheet_file_name)) {
		log.Println("No csv filename extenstion found in provided google sheet filename, adding extension to requested google sheet filename")
		google_sheet_file_name = google_sheet_file_name + ".csv"
	}
	if google_parent_id == "" {
		log.Fatal("Please provide the new google sheets parent folder id using flag '-p'")
	}
	if new_gs_req == false && existing_sheet_id == "" {
		log.Fatal("New Google Sheet has been set to false with the '-n' flag, but no exisiting Google Sheet ID has been provided with 's' flag")
	}

}

func main() {

	// Determine present working directory
	wd, err := os.Getwd()
	error_check(err)

	// Determine which struct to use for unmarshalling json
	struct_req := json_identifier(json_file_path)

	// Create csv file and write data from json file
	err = create_csv(wd, csv_file_name, json_file_path, struct_req)
	error_check(err)
	log.Println("Finished creating csv file with data from json file!")

	// Create google sheet file if requested or add to an existing file with new sheet
	if new_gs_req {
		log.Println("Creating new google sheet named " + google_sheet_file_name)
		sheet_id, err := create_gs(google_sheet_file_name, google_parent_id)
		error_check(err)
		log.Println("Google sheet " + google_sheet_file_name + " created with id " + sheet_id)

		// Upload csv to google sheets
		resp, err := write_to_google_sheet(csv_file_name, google_parent_id, sheet_id, new_gs_req)
		error_check(err)
		log.Println("Finished writing to google sheet " + google_sheet_file_name + " with sheet id " + resp.SpreadsheetId)
	} else {
		// Upload csv to google sheets
		resp, err := write_to_google_sheet(csv_file_name, google_parent_id, existing_sheet_id, new_gs_req)
		error_check(err)
		log.Println("Finished writing to google sheet " + google_sheet_file_name + " with sheet id " + resp.SpreadsheetId)
	}

}

// Func create_csv creates a csv file
func create_csv(wd string, csv_file_name string, json_file string, struct_req string) error {
	// Delete csv file if it exists
	_, err := os.Stat(wd + "/" + csv_file_name)
	if err == nil {
		log.Println("CSV filename " + csv_file_name + " already exists: Removing existing file before proceeding!")
		err = os.Remove(csv_file_name)
		if err != nil {
			return err
		}
		log.Println("Existing CSV file " + csv_file_name + " removed!")
	}
	// Create csv file
	file, err := os.Create(csv_file_name)
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)

	log.Println("CSV file " + csv_file_name + " created")

	log.Println("Begining to retrieve data from json file to write to CSV file")
	err = json_to_csv(json_file, struct_req, w)
	if err != nil {
		return err
	}
	return nil
}

// Func json_to_csv takes a json file and unmarshalls it to be written to a csv file
func json_to_csv(json_file string, struct_req string, w *csv.Writer) error {

	var jpl []PodLatencyStruct
	var jint []JsonStructValInt
	var jfi []JsonStructValFloatInstance
	var jfn []JsonStructValFloatNode
	var key string

	// Read data from json file
	data, err := ioutil.ReadFile(json_file)
	if err != nil {
		return err
	}

	// Unmarshal data by required struct
	if struct_req == "pod_latency_struct" {
		key = "pod_latency"
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jpl)
		if err != nil {
			return err
		}
	} else if struct_req == "json_struct_float64_instance" {
		key = "float_instance"
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfi)
		if err != nil {
			return err
		}
	} else if struct_req == "json_struct_float64_node" {
		key = "float_node"
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfn)
		if err != nil {
			return err
		}
	} else if struct_req == "json_struct_int" {
		key = "int"
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jint)
		if err != nil {
			return err
		}
	} else {
		// Default key and struct
		key = "int"
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jint)
		if err != nil {
			return err
		}
	}

	// Write header depending on struct
	if key == "pod_latency" {
		header := []string{"quantileName", "uuid", "p99", "p95", "p50", "max", "avg", "timestamp", "metricName", "jobName"}
		w.Write(header)
		w.Flush()
	} else {
		header := []string{"JobName", "Node", "MaxValue", "MetricName", "Timestamp", "UUID", "Query"}
		w.Write(header)
		w.Flush()
	}

	var all_job_names []string
	var all_node_names []string

	// Retrieve all node and job namess
	if key == "pod_latency" {
		log.Println("Not ordering data by node by job for this type of csv file")
	} else if key == "float_instance" {
		for _, all := range jfi {
			if !(exists(all_job_names, all.JobName)) {
				all_job_names = append(all_job_names, all.JobName)
			}
		}

		for _, node := range jfi {
			if !(exists(all_node_names, node.Labels.Node)) {
				all_node_names = append(all_node_names, node.Labels.Node)
			}
		}
	} else if key == "float_node" {
		for _, all := range jfn {
			if !(exists(all_job_names, all.JobName)) {
				all_job_names = append(all_job_names, all.JobName)
			}
		}

		for _, node := range jfn {
			if !(exists(all_node_names, node.Labels.Node)) {
				all_node_names = append(all_node_names, node.Labels.Node)
			}
		}
	} else {
		for _, all := range jint {
			if !(exists(all_job_names, all.JobName)) {
				all_job_names = append(all_job_names, all.JobName)
			}
		}

		for _, node := range jint {
			if !(exists(all_node_names, node.Labels.Node)) {
				all_node_names = append(all_node_names, node.Labels.Node)
			}
		}
	}

	// Run simple write to csv for podLatency
	if key == "pod_latency" {
		for _, o := range jpl {
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

	count_nodes := len(all_node_names)
	n := 0

	// Calculate max values by node by job and werite to csv
	for n < count_nodes {
		var jobs_by_node []string

		if key == "float_instance" {
			for _, v := range jfi {
				if v.Labels.Node == all_node_names[n] && !(exists(jobs_by_node, v.JobName)) {
					jobs_by_node = append(jobs_by_node, v.JobName)
				}
			}

			var csv_row []string
			num_jobs := len(jobs_by_node)
			j := 0
			var max float64
			max = 0

			for j < num_jobs {
				var temp []string
				for _, v := range jfi {
					if v.Labels.Node == all_node_names[n] && v.JobName == jobs_by_node[j] {
						if v.Value > max {
							max = v.Value
							temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
						}
					}
				}
				csv_row = temp
				w.Write(csv_row)
				w.Flush()
				j++
			}
			n++
		} else if key == "float_node" {
			for _, v := range jfn {
				if v.Labels.Node == all_node_names[n] && !(exists(jobs_by_node, v.JobName)) {
					jobs_by_node = append(jobs_by_node, v.JobName)
				}
			}

			var csv_row []string
			num_jobs := len(jobs_by_node)
			j := 0
			var max float64
			max = 0

			for j < num_jobs {
				var temp []string
				for _, v := range jfn {
					if v.Labels.Node == all_node_names[n] && v.JobName == jobs_by_node[j] {
						if v.Value > max {
							max = v.Value
							temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
						}
					}
				}
				csv_row = temp
				w.Write(csv_row)
				w.Flush()
				j++
			}
			n++
		} else if key == "int" {
			for _, v := range jint {
				if v.Labels.Node == all_node_names[n] && !(exists(jobs_by_node, v.JobName)) {
					jobs_by_node = append(jobs_by_node, v.JobName)
				}
			}

			var csv_row []string
			num_jobs := len(jobs_by_node)
			j := 0
			max := 0

			for j < num_jobs {
				var temp []string
				for _, v := range jint {
					if v.Labels.Node == all_node_names[n] && v.JobName == jobs_by_node[j] {
						if v.Value > max {
							max = v.Value
							temp = []string{v.JobName, v.Labels.Node, strconv.Itoa(v.Value), v.MetricName, v.Timestamp, v.UUID, v.Query}
						}
					}
				}
				csv_row = temp
				w.Write(csv_row)
				w.Flush()
				j++
			}
			n++
		}
	}
	return nil
}

// Func json_identifier determines what json file is being used to pass in correct struct for unmarsahlling
func json_identifier(json_file string) string {

	var json_struct_req string

	if strings.Contains(json_file, "job-podLatency") {
		json_struct_req = "pod_latency_struct"
		return json_struct_req
	}

	if strings.Contains(json_file, "nodeCPU") {
		json_struct_req = "json_struct_float64_instance"
		return json_struct_req
	}

	if strings.Contains(json_file, "kubeletCPU") || strings.Contains(json_file, "kubeletMemory") {
		json_struct_req = "json_struct_float64_node"
		return json_struct_req
	}
	json_struct_req = "json_struct_int"
	return json_struct_req
}

// Func create_gs creates a new google spreadsheet
func create_gs(google_sheet_file_name string, parent string) (string, error) {
	var r io.Reader

	gdrive_srv, err := gdrive.NewServiceWithCtx(context.TODO())
	if err != nil {
		return "", err
	}

	new_sheet, err := gdrive_srv.CreateFile(google_sheet_file_name, parent, r)
	if err != nil {
		return "", err
	}

	return new_sheet.Id, nil
}

// Func write_to_google_sheets creates a specified google sheet utilizing an existing csv file
func write_to_google_sheet(csv_file string, parent string, sheet_id string, new_gs_req bool) (*sheets.UpdateValuesResponse, error) {
	var empty *sheets.UpdateValuesResponse
	new_sheet_name := csv_file

	// Create gsheet service
	gsheet_srv, err := gsheets.NewServiceWithCtx(context.TODO())
	if err != nil {
		return empty, err
	}

	if new_gs_req {
		default_string := make([][]string, 1)
		default_string[0] = make([]string, 1)
		default_string[0][0] = "Web-Burner Metrics from /home/kni/web-burner/collected-metrics"

		response, err := gsheet_srv.UpdateRangeStrings(sheet_id, "A001", default_string)
		if err != nil {
			return empty, err
		}
		log.Println("Response from updating Sheet1: ", response)
	}

	// Create new sheet
	log.Println("Creating new sheet named " + new_sheet_name + " in google sheed id " + sheet_id)
	err = gsheet_srv.NewSheet(sheet_id, new_sheet_name)
	if err != nil {
		return empty, err
	}

	r, err := os.Open(csv_file)
	if err != nil {
		return empty, err
	}

	log.Println("Attempting to write CSV file", csv_file, "to new sheet")
	resp, err := gsheet_srv.UpdateRangeCSV(sheet_id, new_sheet_name, r)
	if err != nil {
		return empty, err
	}
	log.Println("Successfully wrote CSV file", csv_file, "to new sheet")
	return resp, nil
}

// Func derefString removes the pointer to a string
func derefString(s *string) string {
	if s != nil {
		return *s
	}

	return ""
}

// Func exists checks if an element exists againnt an array
func exists(a []string, element string) bool {
	for _, e := range a {
		if e == element {
			return true
		}
	}
	return false
}

// Func error checks error and exits if is not empty
func error_check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Func check_suffix checks if csv file name has .csv extension and returns bool
func check_suffix(s string) bool {
	resp := strings.HasSuffix(s, ".csv")
	return resp
}
