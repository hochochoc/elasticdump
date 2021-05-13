package utils

import (
	"io/ioutil"
	"os"
	"compress/gzip"
	"bytes"
	"regexp"
	"strconv"
	"bufio"
	"time"
	"strings"
	"fmt"
	"path"
)

func CheckSize(path string) (int64, error){
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	x := info.Size()

	return x, nil
}

func ConvertSizeUnit(inputSize string, inputUnit string) int64 {
	size, _ := strconv.ParseInt(inputSize, 10, 64)
	switch (inputUnit) {
	case "mb","MB","mB","Mb":
		return size*1000000
	}
	return 0
} 

func AppendFile(filePath string, text string) error{
	baseDir := path.Dir(filePath)
	_, err := os.Stat(baseDir)
	if err = os.MkdirAll(baseDir, 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	defer f.Close()

	if _, err = f.WriteString(text+"\n"); err != nil {
		return err
	}
	return nil
}

func DetectInputFileSize(input string) (inputSize string, inputUnit string) {
	r := regexp.MustCompile(`(\d+)(\s+)?([a-zA-Z]+)`)
	match := r.FindStringSubmatch(input)
	if len(match) > 0 {
		return match[1], match[3]
	}
	return 
}

func Gzip(filename string) (filezip string, err error) {
	rawfile, err := os.Open(filename)

	if err != nil {
		return
	}
	defer rawfile.Close()

	// calculate the buffer size for rawfile
	info, _ := rawfile.Stat()

	var size int64 = info.Size()
	rawbytes := make([]byte, size)

	// read rawfile content into buffer
	buffer := bufio.NewReader(rawfile)
	_, err = buffer.Read(rawbytes)

	if err != nil {
		return
	}

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	writer.Write(rawbytes)
	writer.Close()

	filezip = filename+".gz"
	err = ioutil.WriteFile(filename+".gz", buf.Bytes(), info.Mode())
	// use 0666 to replace info.Mode() if you prefer

	if err != nil {
		return
	} 
	os.Remove(filename)
	return
}

func DateToBackup(backoff int) string{
	dt := time.Now()
	backDate := dt.AddDate(0, 0, -backoff)
	return backDate.Format("2006-01-02")
}

func DateFormatES(backoff int) string{
	return fmt.Sprintf("%v.%v.%v", strings.Split(DateToBackup(backoff), "-")[0], strings.Split(DateToBackup(backoff), "-")[1], strings.Split(DateToBackup(backoff), "-")[2])
}