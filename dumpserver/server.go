package dumpserver 

import (
	"elasticdump/utils"
	"os"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"fmt"
	"strings"
	"encoding/json"
	"bytes"
	"github.com/tidwall/gjson"
	"time"
	"log"
	"io"
)

const (
	ROW_SIZE=1000
	BATCH_REQUEST_SIZE=100
)

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}

type DumpServer struct {
	EsClient *elasticsearch.Client
	IndexPattern string 
	ServiceName string 
	OutputType string
	OutputBucket string 
	OutputDir string 
	Compress bool
	FileSize string 
	AccessKeyID string 
	SecretAccessKey string
	LocalDir string
}

func (s *DumpServer) RollQuery() string {
	var query string
	if s.ServiceName != "" {
		query = fmt.Sprintf("{\"query\": {\"term\" : {\"k8s.name.keyword\": \"%v\"}}}", s.ServiceName)
	} else {
		query = fmt.Sprintf("{\"query\": {\"match_all\" : {}}}")
	}
	return query
}

func (s *DumpServer) GetFirstScroll(query string) (data []interface{}, scrollID string, batchNum int, err error){
	var b strings.Builder
	var buf bytes.Buffer
	b.WriteString(query)
	queryRead := strings.NewReader(b.String())
	// Attempt to encode the JSON query and look for errors
	if err = json.NewEncoder(&buf).Encode(queryRead); err != nil {
		log.Println("Error encoding query: ", err)
		return
	} 
	res, err := s.EsClient.Search(
		s.EsClient.Search.WithIndex(s.IndexPattern),
		s.EsClient.Search.WithBody(queryRead),
		s.EsClient.Search.WithSort("_doc"),
		s.EsClient.Search.WithSize(BATCH_REQUEST_SIZE),
		s.EsClient.Search.WithScroll(time.Minute),
	)
	if err != nil {
		return
	}

	// Handle the first batch of data and extract the scrollID
	//
	jsonRead := read(res.Body)
	res.Body.Close()

	scrollID = gjson.Get(jsonRead, "_scroll_id").String()
	
	log.Println("Batch   ", batchNum)
	log.Println("ScrollID", scrollID)
	hits := gjson.Get(jsonRead, "hits.hits")
	var raws []interface{}
	if err = json.Unmarshal([]byte(hits.Raw), &raws); err != nil {
		return 
	}
	return raws, scrollID, batchNum, nil
} 

func (s *DumpServer) Scroll() (err error){
	var uploadFile string
	data, scrollID, batchNum, err := s.GetFirstScroll(s.RollQuery())
	if err != nil {
		log.Println(err)
		return
	}
	if len(data) == 0 {
		return
	}
	var split float64
	split, err = s.OutputData(data, split)
	if err != nil {
		log.Println(err)
		return
	}
	for {
		batchNum++

		// Perform the scroll request and pass the scrollID and scroll duration
		//
		res, err := s.EsClient.Scroll(s.EsClient.Scroll.WithScrollID(scrollID), s.EsClient.Scroll.WithScroll(time.Minute))
		if err != nil {
			log.Println("Error: ", err)
			break
		}
		if res.IsError() {
			log.Println("Error response: ", res)
			break
		}

		jsonRead := read(res.Body)
		res.Body.Close()

		// Extract the scrollID from response
		//
		scrollID = gjson.Get(jsonRead, "_scroll_id").String()

		// Extract the search results
		//
		hits := gjson.Get(jsonRead, "hits.hits")

		// Break out of the loop when there are no results
		//
		if len(hits.Array()) < 1 {
			log.Println("Finished scrolling")
			if s.Compress == true {
				filezip, err := utils.Gzip(s.GetLocalFileName(split)) 
				if err != nil {
					log.Println("Error compress backup file: ", err)
					return err
				}
				uploadFile = filezip
			} else {
				uploadFile = s.GetLocalFileName(split)
			}
			if s.OutputType == "s3" {
				sess, err := s.ConnectAWS()
				if err != nil {
					log.Println("Error when connect to aws: ", err)
					return err
				}
				err = s.UploadS3(sess, uploadFile)
				if err != nil {
					log.Println("Error when upload s3: ", err)
					return err
				}
			}
			break
		} else {
			log.Println("Batch   ", batchNum)
			log.Println("ScrollID", scrollID)
			var raws []interface{}
			_ = json.Unmarshal([]byte(hits.Raw), &raws)
			log.Println("Len ", len(raws))
			log.Println(strings.Repeat("-", 80))
			split, err = s.OutputData(raws, split)
			log.Println(split)
			if err != nil {
				log.Println(err)
				break
			}
		}
	}
	return
}

func (s *DumpServer) OutputData(data []interface{}, split float64) (outputSplit float64, err error){
	outputSplit = split
	var uploadFile string
	for _, hit := range data  {
		doc := hit.(map[string]interface{})
		rankingsJson, _ := json.Marshal(doc)
		nowSize, _ := utils.CheckSize(s.GetLocalFileName(outputSplit))
		if nowSize + ROW_SIZE > utils.ConvertSizeUnit(utils.DetectInputFileSize(s.FileSize)) {
			log.Println("Current size of previous file is ", nowSize)
			if s.Compress == true {
				filezip, err := utils.Gzip(s.GetLocalFileName(outputSplit)) 
				if err != nil {
					log.Println("Error compress backup file: ", err)
					break
				}
				uploadFile = filezip
			} else {
				uploadFile = s.GetLocalFileName(split)
			}
			if s.OutputType == "s3"{
				sess, err := s.ConnectAWS()
				if err != nil {
					log.Println("Error when connect to aws: ", err)
					break
				}
				err = s.UploadS3(sess, uploadFile)
				if err != nil {
					log.Println("Error when upload s3: ", err)
					break
				}
			}				
			outputSplit += 1
			log.Println("Write to new file ", s.GetLocalFileName(outputSplit))
		} 
	
		err = utils.AppendFile(s.GetLocalFileName(outputSplit), string(rankingsJson))
		if err != nil {
			log.Println("Append file: ", err)
			break
		}
	}
	return
}

func (s *DumpServer) GetLocalFileName(split float64) string {
	return fmt.Sprintf("%v%v%v.json", s.LocalDir, s.OutputDir, split)
}

func (s *DumpServer) GetUploadFileName(filename string) string {
	localDirIndex := len(s.LocalDir)
	return filename[localDirIndex:]
}

func (s *DumpServer) UploadS3(sess *session.Session, fileDir string) error {
	// Open the file for use
    file, err := os.Open(fileDir)
    if err != nil {
        return err
    }
    defer file.Close()

    // Get file size and read the file content into a buffer
    fileInfo, _ := file.Stat()
    var size int64 = fileInfo.Size()
    buffer := make([]byte, size)
    file.Read(buffer)
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.OutputBucket),
		ACL:    aws.String("public-read"),
		Key:    aws.String(s.GetUploadFileName(fileDir)),
		Body:   bytes.NewReader(buffer),
	   })
	if err == nil {
		os.Remove(fileDir)
	}
	return err
}

func (s *DumpServer) ConnectAWS() (*session.Session, error) {
	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String("us-west-2"),
			Credentials: credentials.NewStaticCredentials(
				s.AccessKeyID,
				s.SecretAccessKey,
				"", // a token will be created when the session it's used.
			),
		})
	return sess, err
}
