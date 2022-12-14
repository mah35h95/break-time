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

	// jobids := []string{"", "", ""}
	jobids := strings.Split(jobs, "/")

	for i := 0; i < len(jobids); i++ {
		fmt.Printf("%s - Loop Start\n", jobids[i])
		err := PauseJob(jobids[i], metaSvcUrl, bearer)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%s - Loop Complete\n", jobids[i])
	}
}

func PauseJob(dataSourceId string, metaSvcUrl string, bearer string) error {
	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return errors.New("invalid dataSourceId " + dataSourceId)
	}

	path := fmt.Sprintf("%v/callback/sources/%v/technologies/%v/databases/%v/jobs/%v.%v/pause", metaSvcUrl, parts[0], parts[1], parts[2], parts[3], parts[4])

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
