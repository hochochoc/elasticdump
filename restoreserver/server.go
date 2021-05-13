package restoreserver

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"os"
	"bufio"
	"fmt"
	"bytes"
	"time"
	"encoding/json"
	"log"
	"github.com/dustin/go-humanize"
)

type RestoreServer struct {
	DesClient *elasticsearch.Client
	NewIndex string
	BackupFile string
}

type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

var (
	buf bytes.Buffer
	res *esapi.Response
	err error
	raw map[string]interface{}
	blk *bulkResponse

	numItems   int
	numErrors  int
	numIndexed int
	numBatches int
	currBatch  int
	count int = 1000
	batch int = 255
)

func (s *RestoreServer) LoadData() (data []string, err error) {
	file, err := os.Open(s.BackupFile)
    if err != nil {
        return
    }
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
  
    for scanner.Scan() {
        data = append(data, scanner.Text())
    }
	return
}

func (s *RestoreServer) PrepIndex() error {
	if res, err = s.DesClient.Indices.Delete([]string{s.NewIndex}); err != nil {
		return err
	}
	res, err = s.DesClient.Indices.Create(s.NewIndex)
	if err != nil {
		return err
	}
	if res.IsError() {
		fmt.Println(res)
		return fmt.Errorf("res is error")
	}
	return nil
}

func (s *RestoreServer) Restore(data []string) {
	if count%batch == 0 {
		numBatches = (count / batch)
	} else {
		numBatches = (count / batch) + 1
	}
	start := time.Now().UTC()
	for i, each_ln := range data {
		numItems++

		currBatch = i / batch
		if i == count-1 {
			currBatch++
		}
		var data map[string]interface{}
		json.Unmarshal([]byte(each_ln), &data)
		docId := data["_id"]
		docType := data["_type"]
		docSource := data["_source"]
		fmt.Println(docId)
		fmt.Println(docType)
		fmt.Println(docSource)
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%v", "_type": "%v" } }%s`, data["_id"], data["_type"], "\n"))
		dataBytes, err := json.Marshal(data["_source"].(map[string]interface{}))
		if err != nil {
			log.Fatalf("Cannot encode article %s: %s", data["_id"], err)
		}
		dataBytes = append(dataBytes, "\n"...)
		buf.Grow(len(meta) + len(dataBytes))
		buf.Write(meta)
		buf.Write(dataBytes)
		if i > 0 && i%batch == 0 || i == count-1 {
			fmt.Printf("[%d/%d] ", currBatch, numBatches)

			res, err = s.DesClient.Bulk(bytes.NewReader(buf.Bytes()), s.DesClient.Bulk.WithIndex(s.NewIndex))
			if err != nil {
				log.Fatalf("Failure indexing batch %d: %s", currBatch, err)
			}
			// If the whole request failed, print error and mark all documents as failed
			//
			if res.IsError() {
				numErrors += numItems
				if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
				} else {
					log.Printf("  Error: [%d] %s: %s",
						res.StatusCode,
						raw["error"].(map[string]interface{})["type"],
						raw["error"].(map[string]interface{})["reason"],
					)
				}
				// A successful response might still contain errors for particular documents...
				//
			} else {
				if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
				} else {
					for _, d := range blk.Items {
						// ... so for any HTTP status above 201 ...
						//
						if d.Index.Status > 201 {
							// ... increment the error counter ...
							//
							numErrors++

							// ... and print the response status and error information ...
							log.Printf("  Error: [%d]: %s: %s: %s: %s",
								d.Index.Status,
								d.Index.Error.Type,
								d.Index.Error.Reason,
								d.Index.Error.Cause.Type,
								d.Index.Error.Cause.Reason,
							)
						} else {
							// ... otherwise increase the success counter.
							//
							numIndexed++
						}
					}
				}
			}

			// Close the response body, to prevent reaching the limit for goroutines or file handles
			//
			res.Body.Close()

			// Reset the buffer and items counter
			//
			buf.Reset()
			numItems = 0
		}
	}

	dur := time.Since(start)
	if numErrors > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			humanize.Comma(int64(numErrors)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	}
}