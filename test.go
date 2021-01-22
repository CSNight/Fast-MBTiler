package main

import (
	"bufio"
	"encoding/json"
	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
)

type st struct {
	X   uint64 `json:"x"`
	Y   uint64 `json:"y"`
	Z   uint64 `json:"z"`
	Url string `json:"url"`
}

func test() {
	exportRedisToLog()
}
func exportRedisToLog() {
	f, err := os.OpenFile("errTile.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("open file error :", err)
		return
	}
	pool := redis.Pool{
		MaxIdle:     16,
		MaxActive:   32,
		IdleTimeout: 120,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379")
		},
	}
	conn := pool.Get()
	defer func() {
		f.Close()
		pool.Close()
	}()
	replay, err := redis.Strings(conn.Do("hkeys", "fail_list"))
	for tk := range replay {
		st := strings.Replace(replay[tk], "tile_", "", -1)
		st = strings.Replace(st, "_", "/", -1)
		_, _ = f.WriteString(st + "\n")
	}
}
func saveLogToRedis() {
	file, err := os.Open("download-2021-1-18.log")
	if err != nil {
		log.Fatal(err)
	}
	pool := redis.Pool{
		MaxIdle:     16,
		MaxActive:   32,
		IdleTimeout: 120,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379")
		},
	}
	conn := pool.Get()
	defer func() {
		file.Close()
		pool.Close()
	}()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineText := scanner.Text()
		srt := strings.Split(lineText, "/")
		x, err := strconv.ParseUint(srt[1], 10, 32)
		y, err := strconv.ParseUint(srt[2], 10, 32)
		z, err := strconv.ParseUint(srt[0], 10, 32)
		et := st{
			X:   x,
			Y:   y,
			Z:   z,
			Url: "https://api.mapbox.com/v4/mapbox.mapbox-streets-v8,mapbox.country-boundaries-v1/" + lineText + ".vector.pbf?sku=101OxDPvepvQI&access_token=pk.eyJ1IjoiY3NuaWdodCIsImEiOiJjazRqanVydXMwYmtlM2VxODF1NDVtNWlsIn0.eGp2KkdstpJjiKdymjZ3sA",
		}
		key := "tile_" + strconv.FormatUint(et.X, 10) + "_" + strconv.FormatUint(et.Y, 10) + "_" + strconv.FormatUint(et.Z, 10)
		val, _ := json.Marshal(et)
		replay, err := conn.Do("hset", "fail_list", key, string(val[:]))
		if err != nil {
			log.Warnf("redis save tile failure %v", replay)
		}
	}
}
