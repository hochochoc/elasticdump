package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote"

	"elasticdump/dumpserver"
	"elasticdump/restoreserver"
	"elasticdump/utils"

	"github.com/elastic/go-elasticsearch/v8"
)

func main() {
	flag.String("config", "", "config key on consul")
	flag.String("consul-addr", "", "Consul http address")
	flag.String("consul-http-auth", "", "Consul http auth")
	flag.String("type", "backup", "You want to backup or restore")
	flag.String("new-index", "restore", "Enter your new index if you want to restore")
	flag.String("des-es", "", "Enter your es host you want to restore on")
	flag.String("backup-file-path", "test", "Enter your backup file path to restore")
	flag.String("path-to-log", "", "Enter your path to log")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	file, err := os.OpenFile(fmt.Sprintf("%v%v%v", viper.GetString("path-to-log"), "elasticdump.", time.Now().Unix()), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)

	if viper.GetString("type") == "backup" {
		os.Setenv("CONSUL_HTTP_ADDR", viper.GetString("consul-addr"))
		os.Setenv("CONSUL_HTTP_AUTH", viper.GetString("consul-http-auth"))
		dump()
	} else {
		restore()
	}

}

func dump() {
	fmt.Println(viper.GetString("config")) // retrieve value from viper
	valueBytes, e := utils.GetFromConsulKV(viper.GetString("config"))
	if e != nil {
		log.Fatalf("err get consul kv %s", e)
		os.Exit(1)
	} else {
		fmt.Println(string(valueBytes))
		err := utils.LoadConfig("toml", valueBytes, false)
		if err != nil {
			log.Fatalf("err load config %s", err)
			os.Exit(1)
		}
	}
	esServers := viper.GetStringSlice("es_servers")
	cfg := elasticsearch.Config{
		Addresses: esServers,
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("err: %s", err)
		os.Exit(2)
	}
	services := viper.GetStringSlice("services")
	backoffDays := viper.GetInt("backoff_days")
	var indexPattern string
	if viper.GetString("mode") == "dev" {
		indexPattern = viper.GetString("index_pattern")
	} else {
		indexPattern = fmt.Sprintf(viper.GetString("index_pattern"), utils.DateFormatES(7))
	}
	s3Bucket := viper.GetString("s3_bucket")
	s3BasePath := viper.GetString("s3_base_path")
	fileSize := viper.GetString("file_size")
	if fileSize == "" {
		fileSize = "400mb"
	}
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	localDir := "/tmp/"
	for _, srv := range services {
		log.Println(srv)
		log.Println("Backup date: ", utils.DateToBackup(backoffDays))
		var outputDir string
		if srv == "" {
			outputDir = s3BasePath + utils.DateToBackup(backoffDays) + "/"
		} else {
			outputDir = s3BasePath + srv + "/" + utils.DateToBackup(backoffDays) + "/"
		}

		s := &dumpserver.DumpServer{
			EsClient:        client,
			IndexPattern:    indexPattern,
			ServiceName:     srv,
			OutputType:      "s3",
			OutputBucket:    s3Bucket,
			OutputDir:       outputDir,
			Compress:        true,
			FileSize:        fileSize,
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			LocalDir:        localDir,
		}
		err := s.Scroll()
		if err != nil {
			log.Printf("Backup %s failed: %s", srv, err)
			checkpoint(false, utils.DateToBackup(backoffDays))
		} else {
			log.Printf("Backup %s succeeded", srv)
			checkpoint(true, utils.DateToBackup(backoffDays))
		}

	}
}

func checkpoint(status bool, date string) {
	f, err := os.OpenFile("backup_status", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%s,dump=%v\n", date, status)); err != nil {
		log.Println(err)
	}
}

func restore() {
	esServers := []string{viper.GetString("des-es")}
	cfg := elasticsearch.Config{
		Addresses: esServers,
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	s := &restoreserver.RestoreServer{
		DesClient:  client,
		NewIndex:   viper.GetString("new-index"),
		BackupFile: viper.GetString("backup-file-path"),
	}
	err = s.PrepIndex()
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	data, err := s.LoadData()
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	s.Restore(data)
}
