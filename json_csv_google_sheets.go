package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cristoper/gsheet/gdrive"
)

var uuid string
var google_parent_id string
var google_sheet_file_name string
var google_sheet_id string

func init() {

	u := flag.String("uuid", "", "uuid being used for workload")
	p := flag.String("parent", "", "google sheet parent id")
	flag.Parse()

	uuid = derefString(u)
	google_parent_id = derefString(p)

	if uuid == "" {
		log.Fatal("Please provide uuid using flag '-uuid'")
	}
	if google_parent_id == "" {
		log.Fatal("Please provide google parent id using flag '-parent'")

	}
}

func main() {

	// Determine present working directory
	log.Println("Determing pwd")
	wd, err := os.Getwd()
	error_check(err)

	// Determine Date and set to var for file names
	log.Println("Calculating date for csv file anme")
	year, month, day := time.Now().Date()
	store_sheetid := "Sheetid-" + strconv.Itoa(year) + "-" + month.String() + "-" + strconv.Itoa(day) + ".txt"
	google_sheet_file_name = strconv.Itoa(year) + "-" + month.String() + "-" + strconv.Itoa(day) + ".csv"

	// Check for existing google sheet and write to local csv if it exists
	log.Println("Determining if google sheet id exists already from previous runs today to append to")
	google_sheet_id, err = retrieve_sheetid(wd, store_sheetid, google_sheet_file_name)
	error_check(err)

	// Retrieve json files
	json_files := retrieve_json_files(uuid)
	log.Println("Files found:", json_files)

	// Unmarshall json data and write to csv file
	err = csv_file(wd, json_files, uuid, google_sheet_file_name, google_sheet_id)
	error_check(err)

	// Upload csv file
	err = write_to_google_sheet(wd, google_sheet_file_name, google_parent_id, google_sheet_id, store_sheetid)
	error_check(err)
}

// Func retrieve_sheetid checks to see if there is an existing sheet to use and creates a google sheet if necessary
func retrieve_sheetid(wd string, store_sheetid string, file_name string) (string, error) {
	// Create temp dir if it has not been created
	if !(check_file_exists(wd, "/gsheet")) {
		log.Println("No temp dir found, creating temp dir for future sheetid files")
		_, err := exec.Command("bash", "-c", "mkdir temp").Output()
		if err != nil {
			return "", err
		}
	}
	// Check to see if txt file for today exists with sheet id from a previous run
	if check_file_exists(wd, "/gsheet/"+store_sheetid) {
		// Return google sheet id
		log.Println("Google Sheet already exists, retrieving sheet id!")
		cmd := "cat gsheet/" + store_sheetid
		out, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return "", err
		}
		s := string(out)
		sheetid := strings.TrimSpace(s)
		err = gsheet_csv(wd, sheetid, file_name)
		if err != nil {
			return "", err
		}
		return sheetid, nil
	}
	// Create google sheet and return sheet id
	log.Println("No sheet id found, will create new google sheet when uploading!")
	return "", nil
}

// Func gsheet_csv retrieves csv data from google sheet
func gsheet_csv(wd string, sheetid string, file_name string) error {
	f := wd + "/gsheet/" + file_name

	// Delete csv file if it exists with old data
	log.Println("Checking for exisitng csv file with the same name and removing to create a new one with up to date information")
	_, err := os.Stat(f)
	if err == nil {
		log.Println("CSV filename " + file_name + " already exists: Removing existing file before proceeding!")
		err = os.Remove(f)
		if err != nil {
			return err
		}
	}

	log.Println("Creating new csv file " + file_name)
	err = csv_create(f)
	if err != nil {
		return err
	}

	log.Println("Writing retrieved information from google sheet to append to new csv file")
	// Wrtie data from sheet id csv file to local new csv file
	cmd := "gsheet csv --id " + sheetid + " --range \"Sheet1\" > " + f
	log.Println(cmd)
	_, err = exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return err
	}
	return nil
}

// Func retrieve_json_files iterates over collected-metrics directory looking for json files that match uuid
func retrieve_json_files(uuid string) []string {
	var json_fies []string
	files_req := []string{"nodeCPU", "nodeMemoryActive", "nodeMemoryAvailable", "nodeMemoryCached", "kubeletMemory", "kubeletCPU", "crioCPU", "crioMemory"}
	length := len(files_req)
	i := 0
	for i < length {
		command := "ls collected-metrics/ | grep -P '^(?=.*" + uuid + ")(?=.*" + files_req[i] + ")'"
		out, err := exec.Command("bash", "-c", command).Output()
		if err != nil {
			log.Println("File", files_req[i], "with uuid", uuid, "not found with error:", err)
			continue
		}
		json_fies = append(json_fies, string(out))
		i++
	}
	return json_fies
}

// Func create_gs creates a new google spreadsheet
func create_gs(file_name string, parent string) (string, error) {

	var r io.Reader

	gdrive_srv, err := gdrive.NewServiceWithCtx(context.TODO())
	if err != nil {
		return "", err
	}

	new_sheet, err := gdrive_srv.CreateFile(file_name, parent, r)
	if err != nil {
		return "", err
	}

	return new_sheet.Id, nil
}

// Func write_to_google_sheets creates a specified google sheet utilizing an existing csv file
func write_to_google_sheet(wd string, file_name string, parent string, sheet_id string, txt_file string) error {
	f := wd + "/gsheet/" + file_name
	t := wd + "/gsheet/" + txt_file

	if sheet_id == "" {
		s, err := create_gs(file_name, parent)
		if err != nil {
			return err
		}
		sheet_id = s
		// Wrtie data from sheet id csv file to local new csv file
		cmd := "echo " + sheet_id + " > " + t
		_, err = exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return err
		}
	}

	cmd := "cat " + f + " | gsheet csv --id " + sheet_id + " --range 'Sheet1'"
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return err
	}
	return nil
}

// Func derefString removes the pointer to a string
func derefString(s *string) string {
	if s != nil {
		return *s
	}

	return ""
}

// Func check_file_exists checks to see if file exists and returns bool
func check_file_exists(wd string, file string) bool {
	// Delete Summary Page csv file if it exists
	_, err := os.Stat(wd + "/" + file)
	if err == nil {
		return true
	}
	return false
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

func create_sheet_tab_name(j string) string {
	json_file := filepath.Base(j)
	name := strings.TrimSuffix(json_file, filepath.Ext(json_file))
	name = name + ".csv"
	return name
}
