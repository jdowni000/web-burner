package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cristoper/gsheet/gsheets"
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

// Func csv_create creates a csv file
func csv_create(file_name string) error {
	// Create csv file
	_, err := os.Create(file_name)
	if err != nil {
		return err
	}
	return nil
}

// Func write_newsheet creates a new sheet and uploads csv file data
func write_newsheet(csv_file string, sheet_name string, sheet_id string) error {

	// Create gsheet service
	gsheet_srv, err := gsheets.NewServiceWithCtx(context.TODO())
	if err != nil {
		return err
	}

	// Create new sheet
	log.Println("Creating new sheet named " + sheet_name + " in google sheed id " + sheet_id)
	err = gsheet_srv.NewSheet(sheet_id, sheet_name)
	if err != nil {
		return err
	}

	f, err := os.Open(csv_file)
	if err != nil {
		return err
	}

	// log.Println("Attempting to write CSV file", csv_file, "to new sheet"+sheet_name)
	_, err = gsheet_srv.UpdateRangeCSV(sheet_id, sheet_name, f)
	if err != nil {
		return err
	}
	// log.Println("Successfully wrote CSV file", csv_file, "to new sheet", sheet_name, "in google sheet id", sheet_id)
	return nil
}

//
func csv_file(wd string, json_files []string, uuid string, file_name string, sheetid string, iteration string) error {
	var start_time string
	var end_time string
	m := make(map[string]string)
	f := strings.TrimSpace(wd + "/gsheet/" + file_name)

	if sheetid == "" {
		//create csv
		log.Println("No Previous files detected, creating new csv file", f)
		err := csv_create(f)
		if err != nil {
			return err
		}
	}

	// open csv
	file, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)

	if sheetid == "" {
		sum_table := [][]string{{"Iteration", "start_time", "end_time", "UUID", "NodeCPU", "NodeMemoryActive", "NodeMemoryAvailable", "NodeMemoryCached", "KubeletCPU", "KubeletMemory", "CrioCPU", "CrioMemory"}}
		for _, record := range sum_table {
			if err := w.Write(record); err != nil {
				return err
			}
		}

		// err = w.WriteAll(sum_table)
		// if err != nil {
		// 	return err
		// }
		w.Flush()
	}

	log.Println("Attempting to read json files and write data to csv file")

	length := len(json_files)
	i := 0
	for i < length {
		j := json_files[i]
		j_trim := strings.TrimSpace(j)
		j_path := filepath.Join(wd, "/collected-metrics/", j_trim)

		resp, s, e, err := summary_data(j_path)
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
		if resp[0] == "crioCPU" {
			m["CrioCPU"] = resp[1]
		}
		if resp[0] == "crioMemory" {
			m["CrioMemory"] = resp[1]
		}
		start_time = s
		end_time = e
		i++
	}
	log.Println("Finsihed unmarshalling json files and retrieving data. Attempting to write to csv file", f)
	csv_row := [][]string{{iteration, start_time, end_time, uuid, m["NodeCPU"], m["NodeMemoryActive"], m["NodeMemoryAvailable"], m["NodeMemoryCached"], m["KubeletCPU"], m["KubeletMemory"], m["CrioCPU"], m["CrioMemory"]}}
	// for _, record := range csv_row {
	// 	if err := w.Write(record); err != nil {
	// 		return err
	// 	}
	// }

	err = w.WriteAll(csv_row)
	if err != nil {
		return err
	}
	w.Flush()
	return nil
}

// Func summary_data unmarshalls a json file into defined structs and ranges over values to determine the max value
func summary_data(json_file string) ([]string, string, string, error) {
	var empty []string
	var empty_string string
	var jpl []PodLatencyStruct
	var jint []JsonStructValInt
	var jfi []JsonStructValFloatInstance
	var jfn []JsonStructValFloatNode
	var column []string
	var start_time string
	var end_time string
	var max_int int

	json_struct_req, key := json_identifier(json_file)

	// Read data from json file
	data, err := ioutil.ReadFile(json_file)
	if err != nil {
		return empty, empty_string, empty_string, err
	}

	// Unmarshal data by required struct
	if json_struct_req == "pod_latency_struct" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jpl)
		if err != nil {
			return empty, empty_string, empty_string, err
		}
		len := len(jpl)
		max_int = len - 1
	} else if json_struct_req == "json_struct_float64_instance" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfi)
		if err != nil {
			return empty, empty_string, empty_string, err
		}
		len := len(jfi)
		max_int = len - 1
	} else if json_struct_req == "json_struct_float64_node" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jfn)
		if err != nil {
			return empty, empty_string, empty_string, err
		}
		len := len(jfn)
		max_int = len - 1
	} else if json_struct_req == "json_struct_int" {
		// Unmarshal data
		err := json.Unmarshal([]byte(data), &jint)
		if err != nil {
			return empty, empty_string, empty_string, err
		}
		len := len(jint)
		max_int = len - 1
	} else {
		err := json.Unmarshal([]byte(data), &jint)
		if err != nil {
			return empty, empty_string, empty_string, err
		}
		len := len(jint)
		max_int = len - 1
	}

	if key == "nodeCPU" {
		var max float64
		max = 0
		for _, v := range jfi {
			if v.Value > max {
				max = v.Value
				column = []string{"nodeCPU", fmt.Sprintf("%f", (v.Value))}
			}
		}
		start_time = jfi[0].Timestamp
		end_time = jfi[max_int].Timestamp
	} else if key == "nodeMemoryActive" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				column = []string{"nodeMemoryActive", strconv.Itoa(v.Value)}
			}
		}
		start_time = jint[0].Timestamp
		end_time = jint[max_int].Timestamp
	} else if key == "nodeMemoryAvailable" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				column = []string{"nodeMemoryAvailable", strconv.Itoa(v.Value)}
			}
		}
		start_time = jint[0].Timestamp
		end_time = jint[max_int].Timestamp
	} else if key == "nodeMemoryCached" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				column = []string{"nodeMemoryCached", strconv.Itoa(v.Value)}
			}
		}
		start_time = jint[0].Timestamp
		end_time = jint[max_int].Timestamp
	} else if key == "kubeletCPU" {
		var max float64
		max = 0
		for _, v := range jfn {
			if v.Value > max {
				max = v.Value
				column = []string{"kubeletCPU", fmt.Sprintf("%f", (v.Value))}
			}
		}
		start_time = jfn[0].Timestamp
		end_time = jfn[max_int].Timestamp
	} else if key == "kubeletMemory" {
		var max float64
		max = 0
		for _, v := range jfn {
			if v.Value > max {
				max = v.Value
				column = []string{"kubeletMemory", fmt.Sprintf("%f", (v.Value))}
			}
		}
		start_time = jfn[0].Timestamp
		end_time = jfn[max_int].Timestamp
	} else if key == "crioCPU" {
		var max float64
		max = 0
		for _, v := range jfn {
			if v.Value > max {
				max = v.Value
				column = []string{"crioCPU", fmt.Sprintf("%f", (v.Value))}
			}
		}
		start_time = jfn[0].Timestamp
		end_time = jfn[max_int].Timestamp
	} else if key == "crioMemory" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				column = []string{"crioMemory", strconv.Itoa(v.Value)}
			}
		}
		start_time = jint[0].Timestamp
		end_time = jint[max_int].Timestamp
	} else if key == "default" {
		var max int
		max = 0
		for _, v := range jint {
			if v.Value > max {
				max = v.Value
				column = []string{"default", strconv.Itoa(v.Value)}
			}

		}
		start_time = jint[0].Timestamp
		end_time = jint[max_int].Timestamp
	}
	return column, start_time, end_time, nil
}

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
	if strings.Contains(json_file, "crioMemory") {
		json_struct_req = "json_struct_int"
		key = "crioMemory"
		return json_struct_req, key
	}

	json_struct_req = "json_struct_int"
	key = "default"
	return json_struct_req, key
}

// Func max_node_job_vals retrieves max values by job by node for a json file
func max_node_job_vals(wd string, json_files []string, uuid string, sheet_id string) error {

	var jpl []PodLatencyStruct
	var jint []JsonStructValInt
	var jfi []JsonStructValFloatInstance
	var jfn []JsonStructValFloatNode

	length := len(json_files)
	i := 0
	for i < length {

		json_struct_req, job := json_identifier(json_files[i])

		// create sheet name
		sheet_name := job + "-" + uuid
		path_sn := wd + "/gsheet/max-job-val/" + sheet_name + ".csv"

		// Create csv file for
		file, err := os.Create(path_sn)
		if err != nil {
			return err
		}
		defer file.Close()

		w := csv.NewWriter(file)

		j := json_files[i]
		j_trim := strings.TrimSpace(j)
		j_path := filepath.Join(wd, "/collected-metrics/", j_trim)

		// Read data from json file
		data, err := ioutil.ReadFile(j_path)
		if err != nil {
			return err
		}
		// Unmarshal data by required struct
		if json_struct_req == "pod_latency_struct" {
			// Unmarshal data
			err := json.Unmarshal([]byte(data), &jpl)
			if err != nil {
				return err
			}
		} else if json_struct_req == "json_struct_float64_instance" {
			// Unmarshal data
			err := json.Unmarshal([]byte(data), &jfi)
			if err != nil {
				return err
			}
		} else if json_struct_req == "json_struct_float64_node" {
			// Unmarshal data
			err := json.Unmarshal([]byte(data), &jfn)
			if err != nil {
				return err
			}
		} else if json_struct_req == "json_struct_int" {
			// Unmarshal data
			err := json.Unmarshal([]byte(data), &jint)
			if err != nil {
				return err
			}
		} else {
			err := json.Unmarshal([]byte(data), &jint)
			if err != nil {
				return err
			}

		}

		// Write header depending on struct
		if json_struct_req == "pod_latency_struct" {
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
		if json_struct_req == "pod_latency_struct" {
			log.Println("Not ordering data by node by job for this type of csv file")
		} else if json_struct_req == "json_struct_float64_instance" {
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
		} else if json_struct_req == "json_struct_float64_node" {
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
		} else if json_struct_req == "json_struct_int" {
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
		if json_struct_req == "pod_latency_struct" {
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

			if json_struct_req == "json_struct_float64_instance" {
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
			} else if json_struct_req == "json_struct_float64_node" {
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
			} else if json_struct_req == "json_struct_int" {
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
		err = write_newsheet(path_sn, sheet_name, sheet_id)
		if err != nil {
			return err
		}
		i++
	}
	return nil
}
