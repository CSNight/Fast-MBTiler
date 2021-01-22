package main

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/maptile/tilecover"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/teris-io/shortid"
	pb "gopkg.in/cheggaaa/pb.v1"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

//MBTileVersion mbtiles版本号
const MBTileVersion = "1.2"

//Task 下载任务
type Task struct {
	ID                 string
	Name               string
	Description        string
	File               string
	Min                int
	Max                int
	Layers             []Layer
	TileMap            TileMap
	Total              int64
	Current            int64
	Bar                *pb.ProgressBar
	db                 *sql.DB
	workerCount        int
	savePipeSize       int
	bufSize            int
	wg                 sync.WaitGroup
	abort, pause, play chan struct{}
	workers            chan maptile.Tile
	savingpipe         chan Tile
	tileSet            Set
	complete           bool
	outformat          string
	redisPool          redis.Pool
	conn               string
}

//NewTask 创建下载任务
func NewTask(layers []Layer, m TileMap) *Task {
	if len(layers) == 0 {
		return nil
	}
	id, _ := shortid.Generate()

	task := Task{
		ID:      id,
		Name:    m.Name,
		Layers:  layers,
		Min:     m.Min,
		Max:     m.Max,
		TileMap: m,
	}

	for i := 0; i < len(layers); i++ {
		if layers[i].URL == "" {
			layers[i].URL = m.URL
		}
		t := time.Now()
		layers[i].Count = tilecover.CollectionCount(layers[i].Collection, maptile.Zoom(layers[i].Zoom))
		fmt.Println(time.Since(t))
		fmt.Println(layers[i].Zoom, layers[i].Count)
		task.Total += layers[i].Count
	}
	task.abort = make(chan struct{})
	task.pause = make(chan struct{})
	task.play = make(chan struct{})

	task.workerCount = viper.GetInt("task.workers")
	task.savePipeSize = viper.GetInt("task.savepipe")
	task.workers = make(chan maptile.Tile, task.workerCount)
	task.savingpipe = make(chan Tile, task.savePipeSize)
	task.bufSize = viper.GetInt("task.mergebuf")
	task.tileSet = Set{M: make(maptile.Set)}
	task.complete = false
	task.outformat = viper.GetString("output.format")
	task.conn = viper.GetString("output.conn")
	task.redisPool = redis.Pool{
		MaxIdle:     16,
		MaxActive:   32,
		IdleTimeout: 120,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379")
		},
	}
	return &task
}

//Bound 范围
func (task *Task) Bound() orb.Bound {
	bound := orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{-1, -1}}
	for _, layer := range task.Layers {
		for _, g := range layer.Collection {
			bound = bound.Union(g.Bound())
		}
	}
	return bound
}

//Center 中心点
func (task *Task) Center() orb.Point {
	layer := task.Layers[len(task.Layers)-1]
	bound := orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{-1, -1}}
	for _, g := range layer.Collection {
		bound = bound.Union(g.Bound())
	}
	return bound.Center()
}

//MetaItems 输出
func (task *Task) MetaItems() map[string]string {
	b := task.Bound()
	c := task.Center()
	data := map[string]string{
		"id":          task.ID,
		"name":        task.Name,
		"description": task.Description,
		"attribution": `<a href="http://www.atlasdata.cn/" target="_blank">&copy; MapCloud</a>`,
		"basename":    task.TileMap.Name,
		"format":      task.TileMap.Format,
		"type":        task.TileMap.Schema,
		"pixel_scale": strconv.Itoa(TileSize),
		"version":     MBTileVersion,
		"bounds":      fmt.Sprintf(`%f,%f,%f,%f`, b.Left(), b.Bottom(), b.Right(), b.Top()),
		"center":      fmt.Sprintf(`%f,%f,%d`, c.X(), c.Y(), (task.Min+task.Max)/2),
		"minzoom":     strconv.Itoa(task.Min),
		"maxzoom":     strconv.Itoa(task.Max),
		"json":        task.TileMap.JSON,
	}
	return data
}

//SetupMBTileTables 初始化配置MBTile库
func (task *Task) SetupMBTileTables() error {
	if task.File == "" {
		outdir := viper.GetString("output.directory")
		os.MkdirAll(outdir, os.ModePerm)
		task.File = filepath.Join(outdir, fmt.Sprintf("%s.mbtiles", task.Name))
	}
	db, err := sql.Open("sqlite3", task.File)
	if err != nil {
		return err
	}

	err = optimizeConnection(db)
	if err != nil {
		return err
	}

	_, err = db.Exec("create table if not exists tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
	if err != nil {
		return err
	}

	_, err = db.Exec("create table if not exists metadata (name text, value text);")
	if err != nil {
		return err
	}

	_, err = db.Exec("create unique index name on metadata (name);")
	if err != nil {
		return err
	}

	_, err = db.Exec("create unique index tile_index on tiles(zoom_level, tile_column, tile_row);")
	if err != nil {
		return err
	}

	// Load metadata.
	for name, value := range task.MetaItems() {
		_, err := db.Exec("insert into metadata (name, value) values (?, ?)", name, value)
		if err != nil {
			return err
		}
	}

	task.db = db //保存任务的库连接
	return nil
}

//SetupMBTileTables 初始化配置MBTile库
func (task *Task) SetupMysqlTables() error {
	db, err := sql.Open("mysql", task.conn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	_, err = db.Exec("create table if not exists tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data mediumblob);")
	if err != nil {
		return err
	}

	_, err = db.Exec("create table if not exists metadata (name VARCHAR(50) , value mediumtext);")
	if err != nil {
		return err
	}

	//_, err = db.Exec("create unique if not exists index name on metadata (name);")
	//if err != nil {
	//	return err
	//}
	//
	//_, err = db.Exec("create unique if not exists  index tile_index on tiles(zoom_level, tile_column, tile_row);")
	//if err != nil {
	//	return err
	//}

	// Load metadata.
	for name, value := range task.MetaItems() {
		_, err := db.Exec("insert ignore into metadata (name, value) values (?, ?)", name, value)
		if err != nil {
			return err
		}
	}

	task.db = db //保存任务的库连接
	return nil
}

func (task *Task) abortFun() {
	// os.Stdin.Read(make([]byte, 1)) // read a single byte
	// <-time.After(8 * time.Second)
	task.abort <- struct{}{}
}

func (task *Task) pauseFun() {
	// os.Stdin.Read(make([]byte, 1)) // read a single byte
	// <-time.After(3 * time.Second)
	task.pause <- struct{}{}
}

func (task *Task) playFun() {
	// os.Stdin.Read(make([]byte, 1)) // read a single byte
	// <-time.After(5 * time.Second)
	task.play <- struct{}{}
}

//SavePipe 保存瓦片管道
func (task *Task) savePipe() {
	var batch []Tile
	for tile := range task.savingpipe {
		batch = append(batch, tile)
		if len(batch) == task.savePipeSize {
			err := saveToMBTile(batch, task.db, task.outformat)
			if err != nil {
				if strings.Contains(err.Error(), "lock") {
					task.retryConnect()
					saveToMBTile(batch, task.db, task.outformat)
				}
				log.Errorf("save tile to mbtiles db error ~ %s", err)
			}
			log.Infof("save batch complete count %d", len(batch))
			batch = []Tile{}
		}
	}
	if task.complete {
		err := saveToMBTile(batch, task.db, task.outformat)
		if err != nil {
			if strings.Contains(err.Error(), "lock") {
				task.retryConnect()
				saveToMBTile(batch, task.db, task.outformat)
			}
			log.Errorf("save tile to mbtiles db error ~ %s", err)
		}
		log.Infof("save batch complete count %d", len(batch))
		batch = []Tile{}
	}
	task.wg.Done()
}
func (task *Task) retryConnect() {
	if task.db != nil {
		err := task.db.Close()
		if err != nil {
			time.Sleep(time.Millisecond * 500)
			task.retryConnect()
		}
	}
	task.db = nil
	time.Sleep(time.Millisecond * 100)
	err := task.SetupMBTileTables()
	if err != nil {
		time.Sleep(time.Millisecond * 500)
		task.retryConnect()
	}
}

type ErrTile struct {
	X   int    `json:"x"`
	Y   int    `json:"y"`
	Z   int    `json:"z"`
	Res string `json:"res"`
	Url string `json:"url"`
}

func (task *Task) errToRedis(tile maptile.Tile, url string, res string) {
	var conn redis.Conn
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Warnf("redis connection close failure")
		}
	}()
	conn = task.redisPool.Get()
	et := ErrTile{
		X:   int(tile.X),
		Y:   int(tile.Y),
		Z:   int(tile.Z),
		Url: url,
		Res: res,
	}
	key := "tile_" + strconv.Itoa(et.X) + "_" + strconv.Itoa(et.Y) + "_" + strconv.Itoa(et.Y)
	val, _ := json.Marshal(et)
	replay, err := redis.Int64(conn.Do("hset", "fail_list", key, val))
	if err != nil && replay > 0 {
		log.Warnf("redis save tile failure")
	}
}
func (task *Task) cleanFail(t maptile.Tile) {
	var conn redis.Conn
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Warnf("redis connection close failure")
		}
	}()
	conn = task.redisPool.Get()
	key := "tile_" + strconv.Itoa(int(t.X)) + "_" + strconv.Itoa(int(t.Y)) + "_" + strconv.Itoa(int(t.Y))
	_, err := redis.Int64(conn.Do("hdel", "fail_list", key))
	if err != nil {
		return
	}
}
func (task *Task) retry() {
	var conn redis.Conn
	var alls map[string]string
	defer func() {
		for kv := range alls {
			var te ErrTile
			_ = json.Unmarshal([]byte(alls[kv]), &te)
			if te.Res == "nil tile" || te.Res == "resp 404" {
				continue
			}
			conn.Do("hdel", "fail_list", kv)
		}
		err := conn.Close()
		if err != nil {
			log.Warnf("redis connection close failure")
		}
	}()
	conn = task.redisPool.Get()
	alls, err := redis.StringMap(conn.Do("hgetall", "fail_list"))
	if err != nil {
		return
	}
	for kv := range alls {
		var te ErrTile
		err = json.Unmarshal([]byte(alls[kv]), &te)
		if err != nil {
			continue
		}
		if te.Res == "nil tile" || te.Res == "resp 404" {
			continue
		}
		tile := maptile.Tile{
			X: uint32(te.X),
			Y: uint32(te.X),
			Z: maptile.Zoom(uint32(te.Z)),
		}
		select {
		case task.workers <- tile:
			task.wg.Add(1)
			go task.tileFetcher(tile, te.Url, true)
		}
	}
}

//SaveTile 保存瓦片
func (task *Task) saveTile(tile Tile) error {
	defer task.wg.Done()
	err := saveToFiles(tile, filepath.Base(task.File))
	if err != nil {
		log.Errorf("create %v tile file error ~ %s", tile.T, err)
	}
	return nil
}

//tileFetcher 瓦片加载器
func (task *Task) tileFetcher(t maptile.Tile, url string, isRetry bool) {
	defer task.wg.Done()
	defer func() {
		<-task.workers
	}()
	//start := time.Now()
	prep := func(t maptile.Tile, url string) string {
		url = strings.Replace(url, "{x}", strconv.Itoa(int(t.X)), -1)
		url = strings.Replace(url, "{y}", strconv.Itoa(int(t.Y)), -1)
		url = strings.Replace(url, "{z}", strconv.Itoa(int(t.Z)), -1)
		return url
	}
	pbf := prep(t, url)
	resp, err := http.Get(pbf)
	if err != nil {
		task.errToRedis(t, url, err.Error())
		log.Errorf("fetch :%v error, details: %s ~", t, err)
		return
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Warnf("response close failure")
		}
	}()
	if resp.StatusCode != 200 {
		if resp.StatusCode != 404 {
			log.Errorf("fetch %v tile error, status code: %d ~", pbf, resp.StatusCode)
		}
		task.errToRedis(t, url, "resp "+strconv.Itoa(resp.StatusCode))
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		task.errToRedis(t, url, err.Error())
		log.Errorf("read %v tile error ~ %s", t, err)
		return
	}
	if len(body) == 0 {
		task.errToRedis(t, url, "nil tile")
		return //zero byte tiles n
	}
	tile := Tile{
		T: t,
		C: body,
	}

	if task.TileMap.Format == PBF {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, err = zw.Write(body)
		if err != nil {
			log.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			log.Fatal(err)
		}
		tile.C = buf.Bytes()
	}
	//enable savingpipe
	if task.outformat == "mbtiles" || task.outformat == "mysql" {
		task.savingpipe <- tile
	} else {
		task.wg.Add(1)
		_ = task.saveTile(tile)
	}
	if isRetry {
		task.cleanFail(t)
	}
	//secs := time.Since(start).Seconds()
	//fmt.Printf("\ntile %v, %.3fs, %.2f kb, %s ...\n", t, secs, float32(len(body))/1024.0, pbf)
}

//DownloadZoom 下载指定层级
func (task *Task) downloadLayer(layer Layer) {
	bar := pb.New64(layer.Count).Prefix(fmt.Sprintf("Zoom %d : ", layer.Zoom))
	// bar.SetRefreshRate(time.Second)
	bar.Start()
	// bar.SetMaxWidth(300)

	var tilelist = make(chan maptile.Tile, task.bufSize)

	go tilecover.CollectionChannel(layer.Collection, maptile.Zoom(layer.Zoom), tilelist)

	for tile := range tilelist {
		count := bar.Get()
		if count > 0 && count%1000000 == 0 {
			time.Sleep(time.Minute * 2)
		}
		select {
		case task.workers <- tile:
			bar.Increment()
			task.Bar.Increment()
			task.wg.Add(1)
			go task.tileFetcher(tile, layer.URL, false)
		case <-task.abort:
			log.Infof("task %s got canceled.", task.ID)
			close(tilelist)
		case <-task.pause:
			log.Infof("task %s suspended.", task.ID)
			select {
			case <-task.play:
				log.Infof("task %s go on.", task.ID)
			case <-task.abort:
				log.Infof("task %s got canceled.", task.ID)
				close(tilelist)
			}
		}
	}
	task.wg.Wait()
	bar.FinishPrint(fmt.Sprintf("Task %s zoom %d finished ~", task.ID, layer.Zoom))
}

//Download 开启下载任务
func (task *Task) Download() {
	if task.outformat == "mbtiles" {
		err := task.SetupMBTileTables()
		if err != nil {
			log.Errorf("Database connect and prepare error")
			return
		}
	}
	if task.outformat == "mysql" {
		err := task.SetupMysqlTables()
		if err != nil {
			log.Errorf("Database connect and prepare error")
			return
		}
	}
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = task.savePipeSize
	go task.savePipe()
	//g orb.Geometry, minz int, maxz int
	task.Bar = pb.New64(task.Total).Prefix("Task : ")
	// task.Bar.Start()
	go task.printPipe()
	go task.retryLoop()
	for _, layer := range task.Layers {
		task.downloadLayer(layer)
	}
	task.complete = true
	task.wg.Add(1)
	close(task.savingpipe)
	task.wg.Wait()
	for true {
		if len(task.savingpipe) == 0 {
			break
		}
	}
	task.Bar.FinishPrint(fmt.Sprintf("task %s finished ~", task.ID))
}
func (task *Task) printPipe() {
	for true {
		if task.complete {
			break
		}
		time.Sleep(time.Second * 5)
		log.Debugf("pipe size %d", len(task.savingpipe))
	}
}
func (task *Task) retryLoop() {
	for true {
		task.retry()
		time.Sleep(time.Second * 5)
		if task.complete {
			break
		}
	}
}
