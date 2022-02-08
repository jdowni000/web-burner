package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cristoper/gsheet/gsheets"
	"google.golang.org/api/sheets/v4"
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
	gs_id_store := "Sheetid-" + strconv.Itoa(year) + "-" + month.String() + "-" + strconv.Itoa(day) + ".txt"
	google_sheet_file_name = strconv.Itoa(year) + "-" + month.String() + "-" + strconv.Itoa(day) + ".csv"

	// Check for existing google sheet
	log.Println("Determining if google sheet id exists already from previous runs today to append to")
	google_sheet_id, err = retrieve_sheetid(wd, gs_id_store, google_sheet_file_name)
	error_check(err)

	// Retrieve json files
	json_files := retrieve_json_files(uuid)
	log.Println("Files found:", json_files)

	// Unmarshall json data and write to csv file
	err = csv_file(wd, json_files, uuid, google_sheet_file_name, google_sheet_id)
	error_check(err)
	// Populate Data for Summary_Page.csv

	// Create Google Sheet with Summary_Page.csv

	// Create csv file from workloads and push to existing sheetid

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

// Func retrieve_gsheet retrieves csv data from google sheet
func retrieve_gsheet(wd string, sheetid string, file_name string) error {
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
	// Open csv file
	cmd := "gsheet csv --id " + sheetid + " --range 'Sheet1' > " + f
	_, err = exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return err
	}
	return nil
}

// Func retrieve_sheetid checks to see if there is an existing sheet to use and creates a google sheet if necessary
func retrieve_sheetid(wd string, gs_id_store string, file_name string) (string, error) {
	// Create temp dir if it has not been created
	if !(check_file_exists(wd, "/gsheet")) {
		log.Println("No temp dir found, creating temp dir for future sheetid files")
		_, err := exec.Command("bash", "-c", "mkdir temp").Output()
		if err != nil {
			return "", err
		}
	}
	// Check to see if txt file for today exists with sheet id from a previous run
	if check_file_exists(wd, "/gsheet/"+gs_id_store) {
		// Return google sheet id
		log.Println("Google Sheet already exists, retrieving sheet id!")
		cmd := "cat temp/" + gs_id_store
		out, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return "", err
		}
		sheetid := string(out)
		err = retrieve_gsheet(wd, sheetid, file_name)
		if err != nil {
			return "", err
		}
		return sheetid, nil
	}
	// Create google sheet and return sheet id
	log.Println("No sheet id found, will create new google sheet when uploading!")
	return "", nil
}

// google_sheet_id, err := create_gs(google_sheet_file_name, google_parent_id)
// 	if err != nil {
// 		return "", err
// 	}
// 	// Create txt file that holds sheet id for today
// 	cmd := "echo " + google_sheet_id + " > gsheeet/" + gs_id_store
// 	_, err = exec.Command("bash", "-c", cmd).Output()
// 	if err != nil {
// 		return "", err
// 	}
// 	return google_sheet_id, nil

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

// // Func create_gs creates a new google spreadsheet
// func create_gs(google_sheet_file_name string, parent string) (string, error) {

// 	var r io.Reader

// 	gdrive_srv, err := gdrive.NewServiceWithCtx(context.TODO())
// 	if err != nil {
// 		return "", err
// 	}

// 	new_sheet, err := gdrive_srv.CreateFile(google_sheet_file_name, parent, r)
// 	if err != nil {
// 		return "", err
// 	}

// 	return new_sheet.Id, nil
// }

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

func create_sheet_tab_name(j string) string {
	json_file := filepath.Base(j)
	name := strings.TrimSuffix(json_file, filepath.Ext(json_file))
	name = name + ".csv"
	return name
}
