package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/spf13/viper"

	_ "github.com/shaxbee/go-spatialite"
)

//flag
var (
	hf bool
	cf string
)

func init() {
	flag.BoolVar(&hf, "h", false, "this help")
	flag.StringVar(&cf, "c", "conf.toml", "set config `file`")
	// 改变默认的 Usage，flag包中的Usage 其实是一个函数类型。这里是覆盖默认函数实现，具体见后面Usage部分的分析
	flag.Usage = usage
	//InitLog 初始化日志
	log.SetFormatter(&nested.Formatter{
		HideKeys:        true,
		ShowFullLevel:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
		// FieldsOrder: []string{"component", "category"},
	})
	// then wrap the log output with it
	file, err := os.OpenFile("download.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	writers := []io.Writer{file}
	//同时写文件和屏幕
	fileWriter := io.MultiWriter(writers...)
	if err == nil {
		log.SetOutput(fileWriter)
	} else {
		log.Info("failed to log to file.")
	}
	log.SetLevel(log.DebugLevel)
}
func usage() {
	fmt.Fprintf(os.Stderr, `Fast-MBTiler version: Fast-MBTiler/1.0
Usage: Fast-MBTiler [-h] [-c filename]
`)
	flag.PrintDefaults()
}

//initConf 初始化配置
func initConf(cfgFile string) {
	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		log.Warnf("config file(%s) not exist", cfgFile)
	}
	viper.SetConfigType("toml")
	viper.SetConfigFile(cfgFile)
	viper.AutomaticEnv() // read in environment variables that match
	err := viper.ReadInConfig()
	if err != nil {
		log.Warnf("read config file(%s) error, details: %s", viper.ConfigFileUsed(), err)
	}
	viper.SetDefault("app.version", "v 0.1.0")
	viper.SetDefault("app.title", "MapCloud Tiler")
	viper.SetDefault("output.format", "mbtiles")
	viper.SetDefault("output.directory", "output")
	viper.SetDefault("task.workers", 4)
	viper.SetDefault("task.savepipe", 8)
	viper.SetDefault("task.index", 0)
}

//TileMap 瓦片地图类型
type TileMap struct {
	ID          int
	Name        string
	Description string
	Schema      string //no types,maybe "xyz" or "tms"
	Min         int
	Max         int
	Format      string
	JSON        string
	URL         string
	Token       string
	//such as porxy...
}

func main() {
	flag.Parse()
	if hf {
		flag.Usage()
		return
	}
	if cf == "" {
		cf = "conf.toml"
	}
	initConf(cf)
	start := time.Now()
	tm := TileMap{
		Name:   viper.GetString("tm.name"),
		Min:    viper.GetInt("tm.min"),
		Max:    viper.GetInt("tm.max"),
		Format: viper.GetString("tm.format"),
		Schema: viper.GetString("tm.schema"),
		JSON:   viper.GetString("tm.json"),
		URL:    viper.GetString("tm.url"),
	}
	type cfgLayer struct {
		Min     uint32
		Max     uint32
		Geojson string
		URL     string
	}
	var cfgLrs []cfgLayer
	err := viper.UnmarshalKey("lrs", &cfgLrs)
	if err != nil {
		log.Fatal("lrs配置错误")
	}
	var layers []Layer
	for _, lrs := range cfgLrs {
		for z := lrs.Min; z <= lrs.Max; z++ {
			c := loadCollection(lrs.Geojson)
			layer := Layer{
				URL:        lrs.URL,
				Zoom:       z,
				Collection: c,
			}
			layers = append(layers, layer)
		}
	}
	task := NewTask(layers, tm)
	task.Download()
	secs := time.Since(start).Seconds()
	fmt.Printf("\n%.3fs finished...", secs)
}
