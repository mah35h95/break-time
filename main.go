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

		err := ValidateAndRefreshToken(metaSvcUrl, &bearer)
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

				case dice.Reload:
					body = `{}`
					err = dice.ExecuteJobCmd(dataSourceId, metaSvcUrl, bearer, http.MethodPost, dice.Reload, body)

				case dice.Delete:
					err = dice.DeleteJob(dataSourceId, metaSvcUrl, bearer)

				case dice.EditCron:
					cron := "0 0 * * *"
					err = dice.EditCronSchedule(dataSourceId, metaSvcUrl, bearer, cron)

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
func ValidateAndRefreshToken(metaSvcUrl string, bearer *string) error {
	retryCount := 5

	for i := 0; i < retryCount; i++ {
		err := dice.ValidateToken(metaSvcUrl, *bearer)

		if err != nil {
			fmt.Println(err)

			if err.Error() == "403" {
				fmt.Printf("Updating Identity Token...(%d)\n", i+1)

				newBearer := fmt.Sprintf("Bearer %s", auth.GetIdentityToken())
				bearer = &newBearer

				continue
			}

			return err
		}

		return nil
	}

	fmt.Printf("Failed to update Identity Token for %d times\nExiting...\n", retryCount)
	return fmt.Errorf("unable to refresh identity token")
}
