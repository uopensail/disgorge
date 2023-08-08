package config

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConfig_Init(t *testing.T) {
	AppConf.Init("../conf/dev/config.toml")
	data, _ := json.MarshalIndent(AppConf, " ", " ")
	fmt.Print(string(data))
}
