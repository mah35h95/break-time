package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
)

// GcsListResponce - GCS responce struct
type GcsListResponce struct {
	Kind          string   `json:"kind"`
	NextPageToken string   `json:"nextPageToken"`
	Prefixes      []string `json:"prefixes"`
}

func GetFsDirsToClean(bucketName, dataSourceId, bearer string) []string {
	allDirs := []string{}

	parts := strings.Split(dataSourceId, ".")
	if len(parts) != 5 {
		return allDirs
	}

	prefix := fmt.Sprintf("%s/transactions/", strings.ReplaceAll(dataSourceId, ".", "/"))
	pageToken := ""

	count := 1
	for {
		dirs, nextPageToken := getDirs(pageToken, prefix, bearer, bucketName)
		allDirs = append(allDirs, dirs...)

		fmt.Printf("%s: Fetched files %d times", dataSourceId, count)
		count++

		pageToken = nextPageToken
		if pageToken == "" {
			break
		}
	}

	n := 5 // Number of elements to remove from the end

	if len(allDirs) >= n {
		allDirs = allDirs[:len(allDirs)-n]
	} else {
		allDirs = []string{}
	}

	return allDirs
}

func getDirs(pageToken, prefix, bearer, bucketName string) ([]string, string) {
	queryParams := url.Values{
		"delimiter":  []string{"/"},
		"maxResults": []string{fmt.Sprint(math.MaxInt32)},
		"pageToken":  []string{pageToken},
		"prefix":     []string{prefix},
	}

	path := fmt.Sprintf(
		"https://storage.googleapis.com/storage/v1/b/%s/o?%s",
		bucketName,
		queryParams.Encode(),
	)

	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		fmt.Printf("New Request Create: %+v", err)
		return []string{}, ""
	}

	req.Header = http.Header{
		"Authorization": {bearer},
		"Content-Type":  {"application/json"},
	}

	// Send req using http Client
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("Http Do: %+v", err)
		return []string{}, ""
	}
	if res.StatusCode == 403 {
		fmt.Printf("Un-Authorized: %+v", err)
		return []string{}, ""
	}
	defer res.Body.Close()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("Read Body: %+v", err)
		return []string{}, ""
	}

	gcsListRes := GcsListResponce{}
	err = json.Unmarshal(resBody, &gcsListRes)
	if err != nil {
		fmt.Printf("JSON Unmarshaling: %+v", err)
		fmt.Printf("%s", string(resBody))
		return []string{}, ""
	}

	return gcsListRes.Prefixes, gcsListRes.NextPageToken
}
