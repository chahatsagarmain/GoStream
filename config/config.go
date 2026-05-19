package config

import (
	"log"
	"os"
	"strconv"

	"github.com/spf13/viper"
)

var DATA_DIR = "./data"
var MAX_SEGMENT_SIZE = 1024 * 1024    // 1 MB
var MAX_TOPIC_SIZE = 10 * 1024 * 1024 // 10 MB
var SNAPSHOT_DURATION = 60 * 10       // 10 mings

func LoadConfig() {
	viper.AddConfigPath("./")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal(err)
	}

	params := viper.GetStringMapString("params")
	DATA_DIR = params["data_dir"]
	if err = os.MkdirAll(DATA_DIR, 0755); err != nil {
		log.Fatal(err)
	}
	if MAX_SEGMENT_SIZE, err = strconv.Atoi(params["max_segment_size"]); err != nil {
		log.Fatal(err)
	}

	if MAX_TOPIC_SIZE, err = strconv.Atoi(params["max_topic_size"]); err != nil {
		log.Fatal(err)
	}

	if SNAPSHOT_DURATION, err = strconv.Atoi(params["snapshot_duration"]); err != nil {
		log.Fatal(err)
	}

}
