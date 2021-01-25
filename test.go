package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

type st struct {
	X   uint64 `json:"x"`
	Y   uint64 `json:"y"`
	Z   uint64 `json:"z"`
	Url string `json:"url"`
}
type record struct {
	zoom_level  int
	tile_row    int
	tile_column int
	tile_data   []byte
}

func test() {
	exportTileToSqlite(13)
}
func exportTileToSqlite(zoom int) {
	mysql, _ := sql.Open("mysql", "csnight:qnyh@123@tcp(127.0.0.1:3306)/streets-v8")
	sqlite, err := sql.Open("sqlite3", "output/streets-v8.mbtiles")
	if err != nil {
		return
	}
	_, err = sqlite.Exec("PRAGMA synchronous=1")
	if err != nil {
		return
	}
	_, err = sqlite.Exec("PRAGMA locking_mode=EXCLUSIVE")
	if err != nil {
		return
	}
	_, err = sqlite.Exec("PRAGMA journal_mode=OFF")
	if err != nil {
		return
	}
	type MaxIndex struct {
		maxCol int
	}
	var max MaxIndex
	err = mysql.QueryRow("select max(tile_column) as maxCol from tiles where zoom_level=" + strconv.Itoa(zoom)).Scan(&max.maxCol)
	var offset = 0
	var count = 0
	for true {
		rows, err := mysql.Query("select * from tiles where zoom_level=" + strconv.Itoa(zoom) + " and tile_column=" + strconv.Itoa(offset) + " limit " + strconv.Itoa(max.maxCol+1000))
		if err != nil {
			offset++
			continue
		}
		if offset > max.maxCol {
			break
		}
		tx, er := sqlite.Begin()
		if er != nil {
			offset++
			continue
		}
		sqlStr := "insert or ignore into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);"
		var roc = 0
		for rows.Next() {
			var tr record
			rows.Scan(&tr.zoom_level, &tr.tile_column, &tr.tile_row, &tr.tile_data)
			_, err := tx.Exec(sqlStr, tr.zoom_level, tr.tile_column, tr.tile_row, tr.tile_data)
			count++
			roc++
			if err != nil {
				continue
			}
		}
		log.Infof("batch %d complete", roc)
		err = tx.Commit()
		time.Sleep(time.Microsecond * 50)
		if err != nil {
			offset++
			continue
		}
		offset++
	}
	log.Infof("total %d", count)
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
	file, err := os.Open("errTile.txt")
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
		x, err := strconv.ParseUint(srt[0], 10, 32)
		y, err := strconv.ParseUint(srt[1], 10, 32)
		z, err := strconv.ParseUint(srt[2], 10, 32)
		et := st{
			X:   x,
			Y:   y,
			Z:   z,
			Url: "https://api.mapbox.com/v4/mapbox.mapbox-streets-v8,mapbox.country-boundaries-v1/" + srt[2] + "/" + srt[0] + "/" + srt[1] + ".vector.pbf?sku=101OxDPvepvQI&access_token=pk.eyJ1IjoiY3NuaWdodCIsImEiOiJjazRqanVydXMwYmtlM2VxODF1NDVtNWlsIn0.eGp2KkdstpJjiKdymjZ3sA",
		}
		key := "tile_" + strconv.FormatUint(et.X, 10) + "_" + strconv.FormatUint(et.Y, 10) + "_" + strconv.FormatUint(et.Z, 10)
		val, _ := json.Marshal(et)
		replay, err := conn.Do("hset", "fail_list", key, string(val[:]))
		if err != nil {
			log.Warnf("redis save tile failure %v", replay)
		}
	}
}

type ErrTiles struct {
	X   int    `json:"x"`
	Y   int    `json:"y"`
	Z   int    `json:"z"`
	Res string `json:"res"`
	Url string `json:"url"`
}

func redisKeyCorrect() {
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
		pool.Close()
	}()
	replay, _ := redis.StringMap(conn.Do("hgetall", "fail_list"))
	var count = 0
	for tk := range replay {
		st := strings.Replace(tk, "tile_", "", -1)
		xyz := strings.Split(st, "_")
		z, _ := strconv.ParseInt(xyz[2], 10, 64)
		var et ErrTiles
		_ = json.Unmarshal([]byte(replay[tk]), &et)
		if z > 14 {
			conn.Do("hdel", tk)
			key := "tile_" + strconv.FormatUint(uint64(et.X), 10) + "_" + strconv.FormatUint(uint64(et.Y), 10) + "_" + strconv.FormatUint(uint64(et.Z), 10)
			_, err := conn.Do("hset", "fail_list", key, replay[tk][:])
			if err != nil {
				continue
			}
		}
		if et.Res == "nil tile" || et.Res == "resp 404" {
			count++
			continue
		}
		log.Errorf("%d", count)
	}
}
