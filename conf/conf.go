package conf

import (
	"log"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Port int
}

func LoadConfig(path string) (config *Config, err error) {
	_, err = toml.DecodeFile("./conf/config.toml", &config)
	if err != nil {
		log.Fatalln("Error decoding TOML:", err)
		return
	}
	return
}