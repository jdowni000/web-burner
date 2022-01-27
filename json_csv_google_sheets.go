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
var reader io.Reader

// Struct specifically for Pod Latency summary json files
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
type JsonStruct struct {
	Timestamp  string `json:"timestamp"`
	Labels     Inner  `json:"labels"`
	Value      int    `json:"value"`
	UUID       string `json:"uuid"`
	Query      string `json:"query"`
	MetricName string `json:"metricName"`
	JobName    string `json:"jobName"`
}

type Inner struct {
	Node string `json:"instance"`
}

// type maxValue struct {
// 	JobName    string
// 	Node       string
// 	MetricName string
// 	Value      string
// 	Timestamp  string
// 	UUID       string
// 	Query      string
// }

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

	// //unmarshal json into csv file
	struct_req := json_identifier(json_file_path)

	//create csv file
	err = create_csv(wd, csv_file_name, json_file_path, struct_req)
	error_check(err)

	// //create google sheet
	// sheed_id, err := create_gs(google_sheet_name, google_parent_id)
	// error_check(err)

	// //upload csv to google sheets

}

//func create_csv creates a csv file
func create_csv(wd string, csv_file_name string, json_file string, struct_req string) error {
	csv_file_name = csv_file_name + ".csv"
	//Delete csv file if it exists
	_, err := os.Stat(wd + "/" + csv_file_name)
	if err == nil {
		log.Println("CSV filename " + csv_file_name + " already exists: Removing existing file before proceeding!")
		err = os.Remove(csv_file_name)
		if err != nil {
			return err
		}
		log.Println("Existing CSV file removed with the same name")
	}
	//create csv file
	file, err := os.Create(csv_file_name)
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)

	log.Println("CSV file created")

	log.Println("Begining to retrieve data from json file to write to CSV file")
	err = json_to_csv(json_file, struct_req, w)
	if err != nil {
		return err
	}
	return nil
}

//func json_to_csv takes a json file and unmarshalls it to a defined csv file
func json_to_csv(json_file string, struct_req string, w *csv.Writer) error {

	data, err := ioutil.ReadFile(json_file)

	if struct_req == "pod_latency_struct" {
		var pl []PodLatencyStruct
		//unmarshal data
		err := json.Unmarshal([]byte(json_file), &pl)
		if err != nil {
		}
		return err

	}
	var js []JsonStruct
	//unmarshal data
	err = json.Unmarshal([]byte(data), &js)
	if err != nil {
		return err
	}

	err = data_calc(js, w)
	if err != nil {
		return err
	}

	return nil
}

//
func data_calc(jstruct []JsonStruct, w *csv.Writer) error {

	header := []string{"JobName", "Node", "MaxValue", "MetricName", "Timestamp", "UUID", "Query"}
	w.Write(header)
	w.Flush()
	var all_job_names []string
	var all_node_names []string

	// var job_name []string

	for _, all := range jstruct {
		if !(exists(all_job_names, all.JobName)) {
			all_job_names = append(all_job_names, all.JobName)
		}
	}

	for _, node := range jstruct {
		if !(exists(all_node_names, node.Labels.Node)) {
			all_node_names = append(all_node_names, node.Labels.Node)
		}
	}

	count_nodes := len(all_node_names)

	n := 0

	for n < count_nodes {

		var jobs_by_node []string

		for _, v := range jstruct {
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
			for _, v := range jstruct {
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
	return nil
}

// n := 0
// j := 0
// max := 0

//if node name equals this
//and job name equals this
//find values
//append csv_row
//write row to csv

// for n < count_nodes {
// 	// var csv_row []string
// 	var jobs_by_node []string
// 	for _, n := range jstruct {
// 		if n.Labels.Node == all_node_names[n] {
// 			for
// 		}
// 	}

// 	num_jobs := len(jobs_by_node)
// 	log.Println(num_jobs)
// 	log.Println(jobs_by_node)

// i := 0
// max := 0
// for i < num_jobs {
// 	for _, v := range jstruct {
// 		if v.JobName == jobs_by_node[i] || v.Value > max {
// 			max = v.Value
// 			csv_row = append(csv_row, v.JobName, v.Labels.Node, strconv.Itoa(v.Value), v.MetricName, v.Timestamp, v.UUID, v.Query)
// 		}
// 		log.Println(csv_row)
// 		w.Write(csv_row)
// 		w.Flush()
// 	}
// 	i++
// }

// 	n++
// 	w.Flush()
// }

// 	if v.Value > max {
// 		max = v.Value
// 		temp = []string{v.JobName, v.Labels.Node, strconv.Itoa(v.Value), v.MetricName, v.Timestamp, v.UUID, v.Query}
// 	}
// 	return nil
// }

// for j < count_jobs {

// 	var temp_csv []string
// 	for _, jn := range jstruct {
// 		if jn.JobName == all_job_names[j] {
// 			if jn.Value > max {
// 				max = jn.Value
// 				temp_csv = []string{jn.JobName, jn.Labels.Node, strconv.Itoa(jn.Value), jn.MetricName, jn.Timestamp, jn.UUID, jn.Query}
// 			}
// 		}
// 	}
// 	header := []string{"JobName", "Node", "MaxValue", "MetricName", "Timestamp", "UUID", "Query"}
// 	err := w.Write(header)
// 	if err != nil {
// 		return err
// 	}
// 	err = w.Write(temp_csv)
// 	if err != nil {
// 		return err
// 	}
// 	max = 0

// }

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
