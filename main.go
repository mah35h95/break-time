package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mah35h95/break-time/auth"
	"github.com/mah35h95/break-time/dice"
	"github.com/mah35h95/break-time/utils"
)

// CronRange - Has a min max value for a cron string
type CronRange struct {
	Min  int
	Max  int
	Cron string
}

// main - everything started here
func main() {
	fmt.Println("Fetching ENV variables...")

	jobs, ok := os.LookupEnv("JOBS")
	if !ok || len(jobs) == 0 {
		fmt.Println("JOBS env variable is not set in launch.json, aborting...")
		os.Exit(0)
	}

	metaSvcUrl, ok := os.LookupEnv("META_SVC_URL")
	if !ok || len(metaSvcUrl) == 0 {
		fmt.Println("META_SVC_URL env variable is not set in launch.json, aborting...")
		os.Exit(0)
	}

	cmd, ok := os.LookupEnv("CMD")
	if !ok || len(cmd) == 0 {
		fmt.Println("CMD env variable is not set in launch.json, aborting...")
		os.Exit(0)
	}

	chunkSizeString, ok := os.LookupEnv("CHUNK_SIZE")
	if !ok || len(chunkSizeString) == 0 {
		fmt.Println("CHUNK_SIZE env variable is not set in launch.json, hence picking the default value = 5")
		chunkSizeString = "5"
	}

	fmt.Println("Fetching Identity Token...")
	bearer := fmt.Sprintf("Bearer %s", auth.GetIdentityToken())

	allJobIDs := strings.Split(jobs, "/")

	chunkSize, err := strconv.Atoi(chunkSizeString)
	if err != nil {
		chunkSize = 5
	}

	chunkJobIDs := utils.ChunkJobs(allJobIDs, chunkSize)

	for i := 0; i < len(chunkJobIDs); i++ {
		jobIDs := chunkJobIDs[i]

		bearer, err = ValidateAndRefreshToken(metaSvcUrl, bearer)
		if err != nil {
			fmt.Printf("(%d/%d) Jobs have Completed\n", i*chunkSize, len(allJobIDs))
			fmt.Printf("Next run starts from => (%d/%d): %s\n", i*chunkSize+1, len(allJobIDs), jobIDs[0])
			os.Exit(1)
		}

		wg := sync.WaitGroup{}
		wg.Add(len(jobIDs))

		for j := 0; j < len(jobIDs); j++ {
			dataSourceId := jobIDs[j]
			fmt.Printf("(%d/%d): %s - Start\n", (chunkSize*i)+j+1, len(allJobIDs), dataSourceId)

			go func() {
				body := `{}`
				err := error(nil)

				switch cmd {
				case dice.Pause:
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Pause, body)

				case dice.Resume:
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Resume, body)

				case dice.Stop:
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Stop, body)

				case dice.Load:
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Load, body)

				case dice.Lock:
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Lock, body)

				case dice.Unlock:
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Unlock, body)

				case dice.Reload:
					body = `{}`
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Reload, body)

				case dice.Delete:
					err = dice.DeleteJob(dataSourceId, metaSvcUrl, bearer)

				case dice.EditCron:
					cron, cronTimeZone := getCron((chunkSize * i) + j + 1)
					err = dice.EditCronSchedule(dataSourceId, metaSvcUrl, bearer, cron, cronTimeZone)

				case dice.DeleteHydratedRes:
					err = dice.DeleteHydratedResources(dataSourceId, metaSvcUrl, bearer)

				default:
					fmt.Println("CMD provided does not match with predefined cases, aborting...")
					os.Exit(0)
				}

				if err != nil {
					fmt.Println(err)
				}

				fmt.Printf("(%d/%d): %s - Complete\n", (chunkSize*i)+j+1, len(allJobIDs), dataSourceId)
				wg.Done()
			}()
		}

		wg.Wait()
	}

	fmt.Println("All jobs execution complete!")
}

// ValidateAndRefreshToken - validates and refreshed token when required for every batch
func ValidateAndRefreshToken(metaSvcUrl, bearer string) (string, error) {
	newBearer := bearer
	retryCount := 5

	for i := 0; i < retryCount; i++ {
		err := dice.ValidateToken(metaSvcUrl, newBearer)

		if err != nil {
			fmt.Println(err)

			if err.Error() == "403" {
				fmt.Printf("Updating Identity Token...(%d)\n", i+1)
				newBearer = fmt.Sprintf("Bearer %s", auth.GetIdentityToken())
				continue
			}

			return newBearer, err
		}

		return newBearer, nil
	}

	fmt.Printf("Failed to update Identity Token for %d times\nExiting...\n", retryCount)
	return newBearer, fmt.Errorf("unable to refresh identity token")
}

// getCron - returns an increasing cron string
func getCron(value int) (string, string) {
	cronTimeZone := "America/Chicago"

	cronRanges := []CronRange{
		{Min: 1, Max: 100, Cron: "0 0 * * *"},
		{Min: 101, Max: 200, Cron: "30 0 * * *"},
		{Min: 201, Max: 300, Cron: "0 1 * * *"},
		{Min: 301, Max: 400, Cron: "30 1 * * *"},
		{Min: 401, Max: 500, Cron: "0 2 * * *"},
		{Min: 501, Max: 600, Cron: "30 2 * * *"},
		{Min: 601, Max: 700, Cron: "0 3 * * *"},
		{Min: 701, Max: 800, Cron: "30 3 * * *"},
		{Min: 801, Max: 900, Cron: "0 4 * * *"},
		{Min: 901, Max: 1000, Cron: "30 4 * * *"},
	}

	for _, cronRange := range cronRanges {
		if value >= cronRange.Min && value <= cronRange.Max {
			return cronRange.Cron, cronTimeZone
		}
	}

	return "0 0 * * *", cronTimeZone
}
