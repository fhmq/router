package main

import (
	"router/server"

	log "github.com/cihub/seelog"
	"github.com/spf13/viper"
)

const (
	CONFIG_FILE_NAME     = "./conf/app.toml"
	LOG_CONFIG_FILE_NAME = "./conf/log.xml"
)

func init() {
	viper.Reset()
	viper.SetConfigType("toml")
	viper.SetConfigFile(CONFIG_FILE_NAME)
	if e := viper.ReadInConfig(); e != nil {
		panic(e)
	}

	logger, err := log.LoggerFromConfigAsFile(LOG_CONFIG_FILE_NAME)
	if err != nil {
		panic(err)
	}
	log.ReplaceLogger(logger)

}
func main() {
	port := viper.GetString("service.port")
	topic := viper.GetString("service.infoTopic")
	srv := server.NewServer(port, topic)
	srv.Start()
}
