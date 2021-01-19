package main

import (
	"encoding/json"
	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type st struct {
	X   uint64 `json:"x"`
	Y   uint64 `json:"y"`
	Z   uint64 `json:"z"`
	Url string `json:"url"`
}

func ss() {
	lists := []string{}
	pool := redis.Pool{
		MaxIdle:     16,
		MaxActive:   32,
		IdleTimeout: 120,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379")
		},
	}
	conn := pool.Get()
	for t := range lists {
		srt := strings.Split(lists[t], "/")
		x, err := strconv.ParseUint(srt[1], 10, 32)
		y, err := strconv.ParseUint(srt[2], 10, 32)
		z, err := strconv.ParseUint(srt[0], 10, 32)
		et := st{
			X:   x,
			Y:   y,
			Z:   z,
			Url: "https://api.mapbox.com/v4/mapbox.mapbox-streets-v8,mapbox.country-boundaries-v1/" + lists[t] + ".vector.pbf?sku=101OxDPvepvQI&access_token=pk.eyJ1IjoiY3NuaWdodCIsImEiOiJjazRqanVydXMwYmtlM2VxODF1NDVtNWlsIn0.eGp2KkdstpJjiKdymjZ3sA",
		}
		key := "tile_" + strconv.FormatUint(et.X, 10) + "_" + strconv.FormatUint(et.Y, 10) + "_" + strconv.FormatUint(et.Z, 10)
		val, _ := json.Marshal(et)
		replay, err := conn.Do("hset", "fail_list", key, string(val[:]))
		if err != nil {
			log.Warnf("redis save tile failure %v", replay)
		}
	}
}
