package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/uopensail/ulib/commonconfig"
)

type AppConfig struct {
	commonconfig.ServerConfig `json:",inline" toml:",inline"`
	WorkDir                   string `json:"work_dir" toml:"work_dir"`
	LogDir                    string `json:"log_dir" toml:"log_dir"`
}

func (config *AppConfig) Init(configPath string) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
	if _, err = toml.Decode(string(data), config); err != nil {
		panic(err)
	}
}

var AppConf AppConfig
