package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

func main() {

	fmt.Println("Fetching env variables...")

	jobs, ok := os.LookupEnv("JOBS")
	if !ok || len(jobs) == 0 {
		fmt.Println("JOBS env variable is not set in launch.json")
		os.Exit(0)
	}

	metaSvcUrl, ok := os.LookupEnv("META_SVC_URL")
	if !ok || len(metaSvcUrl) == 0 {
		fmt.Println("META_SVC_URL env variable is not set in launch.json")
		os.Exit(0)
	}

	token, ok := os.LookupEnv("TOKEN")
	if !ok || len(token) == 0 {
		fmt.Println("TOKEN env variable is not set in launch.json")
		os.Exit(0)
	}

	bearer := "Bearer " + token

	// jobIDs := []string{"", "", ""}
	jobIDs := strings.Split(jobs, "/")

	for i := 0; i < len(jobIDs); i++ {
		fmt.Printf("(%d/%d): %s - Loop Start\n", i+1, len(jobIDs), jobIDs[i])

		// To Pause Jobs
		// err := PauseJob(jobIDs[i], metaSvcUrl, bearer)

		// To Resume Jobs
		// err := ResumeJob(jobIDs[i], metaSvcUrl, bearer)

		// To Stop Jobs
		// err := StopJob(jobIDs[i], metaSvcUrl, bearer)

		// To Run/Load Jobs
		err := LoadJob(jobIDs[i], metaSvcUrl, bearer)

		// To Reload Jobs
		// err := ReloadJob(jobIDs[i], metaSvcUrl, bearer)

		//! If you don't intend to delete job make sure the below line is commented or the line is removed
		//! Be Care-full and Think Twice before uncommenting
		// To DELETE Jobs
		// err := DeleteJob(jobIDs[i], metaSvcUrl, bearer)

		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("(%d/%d): %s - Loop Complete\n", i+1, len(jobIDs), jobIDs[i])
	}
}

func PauseJob(dataSourceId string, metaSvcUrl string, bearer string) error {
	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return errors.New("invalid dataSourceId " + dataSourceId)
	}

	path := fmt.Sprintf("%v/sources/%v/technologies/%v/databases/%v/jobs/%v.%v/pause", metaSvcUrl, parts[0], parts[1], parts[2], parts[3], parts[4])

	body := strings.NewReader(`{}`)

	request, err := http.NewRequest("POST", path, body)
	if err != nil {
		return fmt.Errorf("http.NewRequest: %v", err)
	}

	request.Header = http.Header{
		"Authorization": {bearer},
		"Content-Type":  {"application/json"},
	}

	// Send req using http Client
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("client.Do: %v", err)
	}
	defer response.Body.Close()

	fmt.Printf("Job %s has been triggered to be paused.\n", dataSourceId)

	return nil
}

func StopJob(dataSourceId string, metaSvcUrl string, bearer string) error {
	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return errors.New("invalid dataSourceId " + dataSourceId)
	}

	path := fmt.Sprintf("%v/sources/%v/technologies/%v/databases/%v/jobs/%v.%v/stop", metaSvcUrl, parts[0], parts[1], parts[2], parts[3], parts[4])

	body := strings.NewReader(`{}`)

	request, err := http.NewRequest("POST", path, body)
	if err != nil {
		return fmt.Errorf("http.NewRequest: %v", err)
	}

	request.Header = http.Header{
		"Authorization": {bearer},
		"Content-Type":  {"application/json"},
	}

	// Send req using http Client
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("client.Do: %v", err)
	}
	defer response.Body.Close()

	fmt.Printf("Job %s has been triggered to be paused.\n", dataSourceId)

	return nil
}

func ResumeJob(dataSourceId string, metaSvcUrl string, bearer string) error {
	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return errors.New("invalid dataSourceId " + dataSourceId)
	}

	path := fmt.Sprintf("%v/sources/%v/technologies/%v/databases/%v/jobs/%v.%v/resume", metaSvcUrl, parts[0], parts[1], parts[2], parts[3], parts[4])

	body := strings.NewReader(`{}`)

	request, err := http.NewRequest("POST", path, body)
	if err != nil {
		return fmt.Errorf("http.NewRequest: %v", err)
	}

	request.Header = http.Header{
		"Authorization": {bearer},
		"Content-Type":  {"application/json"},
	}

	// Send req using http Client
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("client.Do: %v", err)
	}
	defer response.Body.Close()

	fmt.Printf("Job %s has been triggered to be paused.\n", dataSourceId)

	return nil
}

func LoadJob(dataSourceId string, metaSvcUrl string, bearer string) error {
	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return errors.New("invalid dataSourceId " + dataSourceId)
	}

	path := fmt.Sprintf("%v/sources/%v/technologies/%v/databases/%v/jobs/%v.%v/load", metaSvcUrl, parts[0], parts[1], parts[2], parts[3], parts[4])

	body := strings.NewReader(`{}`)

	request, err := http.NewRequest("POST", path, body)
	if err != nil {
		return fmt.Errorf("http.NewRequest: %v", err)
	}

	request.Header = http.Header{
		"Authorization": {bearer},
		"Content-Type":  {"application/json"},
	}

	// Send req using http Client
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("client.Do: %v", err)
	}
	defer response.Body.Close()

	fmt.Printf("Job %s has been triggered to be run.\n", dataSourceId)

	return nil
}

func ReloadJob(dataSourceId string, metaSvcUrl string, bearer string) error {
	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return errors.New("invalid dataSourceId " + dataSourceId)
	}

	path := fmt.Sprintf("%v/sources/%v/technologies/%v/databases/%v/jobs/%v.%v/reload", metaSvcUrl, parts[0], parts[1], parts[2], parts[3], parts[4])

	body := strings.NewReader(`{}`)

	request, err := http.NewRequest("POST", path, body)
	if err != nil {
		return fmt.Errorf("http.NewRequest: %v", err)
	}

	request.Header = http.Header{
		"Authorization": {bearer},
		"Content-Type":  {"application/json"},
	}

	// Send req using http Client
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("client.Do: %v", err)
	}
	defer response.Body.Close()

	fmt.Printf("Job %s has been triggered to be Reloaded.\n", dataSourceId)

	return nil
}

func DeleteJob(dataSourceId string, metaSvcUrl string, bearer string) error {
	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return errors.New("invalid dataSourceId " + dataSourceId)
	}

	path := fmt.Sprintf("%v/sources/%v/technologies/%v/databases/%v/jobs/%v.%v", metaSvcUrl, parts[0], parts[1], parts[2], parts[3], parts[4])

	body := strings.NewReader(`{}`)

	request, err := http.NewRequest("DELETE", path, body)
	if err != nil {
		return fmt.Errorf("http.NewRequest: %v", err)
	}

	request.Header = http.Header{
		"Authorization": {bearer},
		"Content-Type":  {"application/json"},
	}

	// Send req using http Client
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("client.Do: %v", err)
	}
	defer response.Body.Close()

	fmt.Printf("Job %s has been triggered to be deleted.\n", dataSourceId)

	return nil
}
