package utils

import (
	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote"
	"fmt"
	"bytes"
	"github.com/hashicorp/consul/api"
)

func GetFromConsulKV(key string) (value []byte, err error) {
	// init config connection to consul
	config := api.DefaultConfig()

	// init consul client
	client, err := api.NewClient(config)
	if err != nil {
		return
	}
	kv := client.KV()

	// get key
	pair, _, err := kv.Get(key, nil)
	if err != nil {
		return
	}
	if pair == nil {
		err = fmt.Errorf("remote config key is not existed: %v", key)
		return
	}

	value = pair.Value
	return
}

// LoadConfig -- read config from byte
func LoadConfig(configType string, value []byte, isMerge bool) (err error) {
	viper.SetConfigType(configType)
	if !isMerge {
		err = viper.ReadConfig(bytes.NewBuffer(value))
	} else {
		err = viper.MergeConfig(bytes.NewBuffer(value))
	}
	return
}

