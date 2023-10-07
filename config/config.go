package config

import (
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/uopensail/ulib/commonconfig"
)

type AppConfig struct {
	commonconfig.ServerConfig `json:",inline" toml:",inline"`
	WorkDir                   string `json:"work_dir" toml:"work_dir"`
	LogDir                    string `json:"log_dir" toml:"log_dir"`
}

func (conf *AppConfig) Init(filePath string) {
	fData, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Errorf("ioutil.ReadFile error: %s", err)
		panic(err)
	}
	_, err = toml.Decode(string(fData), conf)
	if err != nil {
		fmt.Errorf("Unmarshal error: %s", err)
		panic(err)
	}
	fmt.Printf("InitAppConfig:%v yaml:%s\n", conf, string(fData))
}

var AppConfigInstance AppConfig
