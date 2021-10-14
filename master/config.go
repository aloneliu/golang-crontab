package master

import (
	"encoding/json"
	"io/ioutil"
)

var (
	// 单例
	G_config *Config
)

type Config struct {
	ApiPort         int      `json:"apiPort"`
	ReadTimeout     int      `json:"readTimeout"`
	WriteTimeout    int      `json:"writeTimeout"`
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
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
