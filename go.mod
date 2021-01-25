module Fast-MBTiler

go 1.15

require (
	github.com/antonfisher/nested-logrus-formatter v1.3.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gomodule/redigo v1.8.3
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/mattn/go-sqlite3 v1.14.2
	github.com/paulmach/orb v0.2.1
	github.com/shaxbee/go-spatialite v0.0.0-20180425212100-9b4c81899e0e
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/viper v1.7.1
	github.com/teris-io/shortid v0.0.0-20171029131806-771a37caa5cf
	gopkg.in/cheggaaa/pb.v1 v1.0.28
)

replace github.com/paulmach/orb v0.2.1 => github.com/atlasdatatech/orb v0.2.2
