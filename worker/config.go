package worker

import (
	"encoding/json"
	"io/ioutil"
)

var (
	// 单例
	G_config *Config
)

type Config struct {
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
	JobLogBatchSize       int      `json:"jobLogBatchSize"`
	JobLogCommitTimeout   int      `json:"jobLogCommitTimeout"`
}

func InitConfig(filename string) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	var conf Config

	err = json.Unmarshal(file, &conf)

	if err != nil {
		return err
	}

	G_config = &conf
	return nil
}
