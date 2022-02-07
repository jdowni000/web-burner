package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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

// Func summary_page creates a csv file in the working directory
func summary_page(wd string, json_files []string, uuid string) error {

	m := make(map[string]string)

	// Iteration     string
	// Start_Time    string
	// End_Time      string
	// UUID          string
	// NodeCPU       int
	// NodeMemory    int
	// KubeletMemory int
	// KubeletCPU    int
	// Crio          int

	// Delete Summary Page csv file if it exists
	_, err := os.Stat(wd + "/" + "Summary_Page.csv")
	if err == nil {
		log.Println("CSV filename Summary_Page.csv already exists: Removing existing file before proceeding!")
		err = os.Remove("Summary_Page.csv")
		if err != nil {
			return err
		}
		log.Println("Existing CSV file Summary_Page.csv removed!")
	}

	// Create csv file
	file, err := os.Create("Summary_Page.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)

	sum_table := []string{"Iteration", "start_time", "end_time", "UUID", "NodeCPU", "NodeMemoryActive", "NodeMemoryAvailable", "NodeMemoryCached", "KubeletCPU", "KubeletMemory", "crio"}
	err = w.Write(sum_table)
	if err != nil {
		log.Fatal(err)
	}
	w.Flush()

	length := len(json_files)
	i := 0
	for i < length {
		j := json_files[i]
		j_trim := strings.TrimSpace(j)
		j_path := filepath.Join(wd, "/collected-metrics/", j_trim)

		resp, err := summary_data(j_path)
		log.Println(resp)
		if err != nil {
			log.Println("Problem parsing json file", j, "with error", err)
		}
		if resp[0] == "nodeCPU" {
			m["NodeCPU"] = resp[1]
		}
		if resp[0] == "nodeMemoryActive" {
			m["NodeMemoryActive"] = resp[1]
		}
		if resp[0] == "nodeMemoryAvailable" {
			m["NodeMemoryAvailable"] = resp[1]
		}
		if resp[0] == "nodeMemoryCached" {
			m["NodeMemoryCached"] = resp[1]
		}
		if resp[0] == "kubeletCPU" {
			m["KubeletCPU"] = resp[1]
		}
		if resp[0] == "kubeletMemory" {
			m["KubeletMemory"] = resp[1]
		}
		if resp[0] == "crio" {
			m["Crio"] = resp[1]
		}
		i++
	}
	csv_row := []string{"", "", uuid, m["NodeCPU"], m["NodeMemoryActive"], m["NodeMemoryAvailable"], m["NodeMemoryCached"], m["KubeletCPU"], m["KubeletMemory"], m["Crio"]}
	err = w.Write(csv_row)
	if err != nil {
		log.Fatal(err)
	}
	w.Flush()
	return nil
}

// Func summary_data unmarshalls a json file into defined structs and ranges over values to determine the max value
func summary_data(json_file string) ([]string, error) {
	var empty []string
	var jpl []PodLatencyStruct
	var jint []JsonStructValInt
	var jfi []JsonStructValFloatInstance
	var jfn []JsonStructValFloatNode
	var column []string

	json_struct_req, key := json_identifier(json_file)
	log.Println(json_struct_req, key)

	// Read data from json file
	log.Println("Attempting to read " + json_file)
	data, err := ioutil.ReadFile(json_file)
	if err != nil {
		return empty, err
	}

	// Unmarshal data by required struct
	if json_struct_req == "pod_latency_struct" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jpl)
		if err != nil {
			return empty, err
		}
	} else if json_struct_req == "json_struct_float64_instance" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfi)
		if err != nil {
			return empty, err
		}
	} else if json_struct_req == "json_struct_float64_node" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfn)
		if err != nil {
			return empty, err
		}
	} else if json_struct_req == "json_struct_int" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jint)
		if err != nil {
			return empty, err
		}
	} else {
		err := json.Unmarshal([]byte(data), &jint)
		if err != nil {
			return empty, err
		}
	}

	if key == "nodeCPU" {
		var max float64
		max = 0
		for _, v := range jfi {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"nodeCPU", fmt.Sprintf("%f", (v.Value))}
			}

		}
	} else if key == "nodeMemoryActive" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"nodeMemoryActive", strconv.Itoa(v.Value)}
			}

		}
	} else if key == "nodeMemoryAvailable" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"nodeMemoryAvailable", strconv.Itoa(v.Value)}
			}

		}
	} else if key == "nodeMemoryCached" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"nodeMemoryCached", strconv.Itoa(v.Value)}
			}

		}
	} else if key == "kubeletCPU" {
		var max float64
		max = 0
		for _, v := range jfn {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"kubeletCPU", fmt.Sprintf("%f", (v.Value))}
			}

		}
	} else if key == "kubeletMemory" {
		var max float64
		max = 0
		for _, v := range jfn {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"kubeletMemory", fmt.Sprintf("%f", (v.Value))}
			}

		}
	} else if key == "crioCPU" {
		var max float64
		max = 0
		for _, v := range jfn {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"crioCPU", fmt.Sprintf("%f", (v.Value))}
			}

		}
	} else if key == "default" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				// temp = []string{v.JobName, v.Labels.Node, fmt.Sprintf("%f", (v.Value)), v.MetricName, v.Timestamp, v.UUID, v.Query}
				column = []string{"default", strconv.Itoa(v.Value)}
			}

		}
	}
	return column, nil
}

// // Func create_csv creates a csv file
// func create_csv(wd string, csv_file_name string, json_file string, key string, struct_req string) error {
// 	// Delete csv file if it exists
// 	_, err := os.Stat(wd + "/" + csv_file_name)
// 	if err == nil {
// 		log.Println("CSV filename " + csv_file_name + " already exists: Removing existing file before proceeding!")
// 		err = os.Remove(csv_file_name)
// 		if err != nil {
// 			return err
// 		}
// 		log.Println("Existing CSV file " + csv_file_name + " removed!")
// 	}
// 	// Create csv file
// 	file, err := os.Create(csv_file_name)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	w := csv.NewWriter(file)

// 	log.Println("CSV file " + csv_file_name + " created")

// 	log.Println("Begining to retrieve data from json file to write to CSV file")
// 	err = json_to_csv(json_file, struct_req, w)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// Func json_identifier determines what json file is being used to pass in correct struct for unmarsahlling
func json_identifier(json_file string) (string, string) {

	var json_struct_req string
	var key string

	if strings.Contains(json_file, "job-podLatency") {
		json_struct_req = "pod_latency_struct"
		key = "pod_latency"
		return json_struct_req, key
	}
	if strings.Contains(json_file, "nodeCPU") {
		json_struct_req = "json_struct_float64_instance"
		key = "nodeCPU"
		return json_struct_req, key
	}
	if strings.Contains(json_file, "nodeMemoryActive") {
		json_struct_req = "json_struct_int"
		key = "nodeMemoryActive"
		return json_struct_req, key
	}
	if strings.Contains(json_file, "nodeMemoryAvailable") {
		json_struct_req = "json_struct_int"
		key = "nodeMemoryAvailable"
		return json_struct_req, key
	}
	if strings.Contains(json_file, "nodeMemoryCached") {
		json_struct_req = "json_struct_int"
		key = "nodeMemoryCached"
		return json_struct_req, key
	}
	if strings.Contains(json_file, "kubeletCPU") {
		json_struct_req = "json_struct_float64_node"
		key = "kubeletCPU"
		return json_struct_req, key
	}
	if strings.Contains(json_file, "kubeletMemory") {
		json_struct_req = "json_struct_float64_node"
		key = "kubeletMemory"
		return json_struct_req, key
	}
	if strings.Contains(json_file, "crioCPU") {
		json_struct_req = "json_struct_float64_node"
		key = "crioCPU"
		return json_struct_req, key
	}

	json_struct_req = "json_struct_int"
	key = "default"
	return json_struct_req, key
}

// Func json_to_csv takes a json file and unmarshalls it to be written to a csv file
func json_to_csv(json_file string, struct_req string, key string, w *csv.Writer) error {

	var jpl []PodLatencyStruct
	var jint []JsonStructValInt
	var jfi []JsonStructValFloatInstance
	var jfn []JsonStructValFloatNode

	// Read data from json file
	data, err := ioutil.ReadFile(json_file)
	if err != nil {
		return err
	}

	// Unmarshal data by required struct
	if struct_req == "pod_latency_struct" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jpl)
		if err != nil {
			return err
		}
	} else if struct_req == "json_struct_float64_instance" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfi)
		if err != nil {
			return err
		}
	} else if struct_req == "json_struct_float64_node" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfn)
		if err != nil {
			return err
		}
	} else if struct_req == "json_struct_int" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jint)
		if err != nil {
			return err
		}
	} else {
		// Default struct
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
