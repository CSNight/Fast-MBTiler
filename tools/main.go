package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	nested "github.com/antonfisher/nested-logrus-formatter"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
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
type exportRec struct {
	wg         sync.WaitGroup
	workers    chan cur
	savingpipe chan []record
	complete   bool
}
type cur struct {
	zoom   int
	column int
	max    int
}

func main() {
	exportTileToSqlite(15)
	//rdbSync()
}
func rdbSync() {
	poolTar := redis.Pool{
		MaxIdle:     16,
		MaxActive:   32,
		IdleTimeout: 120,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379")
		},
	}
	connTar := poolTar.Get()
	poolSou := redis.Pool{
		MaxIdle:     16,
		MaxActive:   32,
		IdleTimeout: 120,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:10001")
		},
	}
	connSou := poolSou.Get()
	defer func() {
		connTar.Close()
		poolTar.Close()
		connSou.Close()
		poolSou.Close()
	}()
	replay, _ := redis.StringMap(connSou.Do("hgetall", "nil_list:90a8b6b4-dfa6-46bc-ac8c-3e3bdc2f429c"))
	var count = 0
	for tk := range replay {
		rep, err := redis.Int(connTar.Do("hset", "nil_list:7c105526-ce46-4432-b090-752b59a113ed", tk, replay[tk][:]))
		if err != nil {
			log.Warnf("%v", err)
		}
		count = count + rep
	}
	log.Warnf("redis save rec %d", count)
}
func exportTileToSqlite(zoom int) {
	log.SetFormatter(&nested.Formatter{
		HideKeys:        true,
		ShowFullLevel:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
		// FieldsOrder: []string{"component", "category"},
	})
	// then wrap the log output with it
	file, err := os.OpenFile("export.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	writers := []io.Writer{file, os.Stdout}
	//同时写文件和屏幕
	fileWriter := io.MultiWriter(writers...)
	if err == nil {
		log.SetOutput(fileWriter)
	} else {
		log.Info("failed to log to file.")
	}
	log.SetLevel(log.DebugLevel)
	task := exportRec{}
	task.workers = make(chan cur, 8)
	task.savingpipe = make(chan []record, 16)
	mysql, _ := sql.Open("sqlite3", "G:\\streets-v8.mbtiles")
	sqlite, err := sql.Open("sqlite3", "G:\\streets-v8\\streets-v8.mbtiles")
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
	_, err = sqlite.Exec("PRAGMA page_size=4096")
	if err != nil {
		return
	}
	_, err = sqlite.Exec("PRAGMA cache_size=8000")
	if err != nil {
		return
	}
	type MaxIndex struct {
		maxCol int
	}
	var max MaxIndex
	err = mysql.QueryRow("select max(tile_column) as maxCol from tiles where zoom_level=" + strconv.Itoa(zoom)).Scan(&max.maxCol)
	var offset = 6800
	var count = 0
	go task.savePipe(sqlite)
	for true {
		if offset > max.maxCol {
			break
		}
		cursor := cur{
			zoom:   zoom,
			column: offset,
			max:    max.maxCol,
		}
		select {
		case task.workers <- cursor:
			task.wg.Add(1)
			go task.genRec(mysql, cursor)
			offset++
			count++
		}
		time.Sleep(time.Millisecond * 100)
	}
	task.wg.Wait()
	close(task.savingpipe)
	for true {
		if task.complete {
			break
		}
	}
	log.Infof("total %d", count)
}
func (task *exportRec) genRec(mysql *sql.DB, cursor cur) {
	defer task.wg.Done()
	defer func() {
		<-task.workers
	}()
	rows, err := mysql.Query("select * from tiles where zoom_level=" + strconv.Itoa(cursor.zoom) + " and tile_column=" + strconv.Itoa(cursor.column) + " limit " + strconv.Itoa(40000))
	if err != nil {
		return
	}
	var recs []record
	for rows.Next() {
		var tr record
		rows.Scan(&tr.zoom_level, &tr.tile_column, &tr.tile_row, &tr.tile_data)
		recs = append(recs, tr)
	}
	if len(recs) > 0 {
		task.savingpipe <- recs
	}
}
func (task *exportRec) savePipe(db *sql.DB) {
	for rec := range task.savingpipe {
		err := task.saveToSqlite(rec, db)
		if err != nil {
			log.Errorf("save tile to mbtiles db error ~ %s", err)
		}
	}
	task.complete = true
}
func (task *exportRec) saveToSqlite(rows []record, sqlite *sql.DB) error {
	start := time.Now()
	tx, er := sqlite.Begin()
	if er != nil {
		return er
	}
	sqlStr := "insert or ignore into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);"
	var roc = 0
	for rec := range rows {
		_, err := tx.Exec(sqlStr, rows[rec].zoom_level, rows[rec].tile_column, rows[rec].tile_row, rows[rec].tile_data)
		roc++
		if err != nil {
			continue
		}
	}
	err := tx.Commit()
	log.Infof("offset %d,batch %d complete,cost %d", rows[0].tile_column, roc, time.Since(start).Milliseconds())
	if err != nil {
		return err
	}
	return nil
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
