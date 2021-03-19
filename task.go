package main

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	pb "gopkg.in/cheggaaa/pb.v1"
	"io"
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

type State int

const (
	Initialize State = iota
	Running
	Pause
	Ending
	Aborting
	Terminated
)

//Task 下载任务
type Task struct {
	ID                 string
	Name               string
	Description        string
	File               string
	MinZoom            int
	MaxZoom            int
	CurCol             int
	CurZoom            int
	StartCol           int
	Layers             []TileOption
	TileMap            TileMap
	Total              int
	db                 *sql.DB
	workerCount        int
	savePipeSize       int
	wg                 sync.WaitGroup
	abort, pause, play chan struct{}
	workers            chan TileXyz
	savingpipe         chan Tile
	signal             State
	outformat          string
	redisPool          redis.Pool
	conn               string
}

//NewTask 创建下载任务
func NewTask(layers []TileOption, m TileMap, id string) (*Task, error) {
	if len(layers) == 0 {
		return nil, errors.New("empty layer")
	}
	task := Task{
		ID:           uuid.New().String(),
		Name:         m.Name,
		Layers:       layers,
		MinZoom:      m.Min,
		MaxZoom:      m.Max,
		CurCol:       0,
		CurZoom:      m.Min,
		StartCol:     -1,
		TileMap:      m,
		signal:       Initialize,
		abort:        make(chan struct{}),
		pause:        make(chan struct{}),
		play:         make(chan struct{}),
		workerCount:  viper.GetInt("task.workers"),
		savePipeSize: viper.GetInt("task.savepipe"),
		outformat:    viper.GetString("output.format"),
		conn:         viper.GetString("output.conn"),
		redisPool: redis.Pool{
			MaxIdle:     16,
			MaxActive:   32,
			IdleTimeout: 120,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", "127.0.0.1:6379")
			},
		},
	}
	if id != "" {
		task.ID = id
	}
	cz, cx := task.getCursor()
	if cz != -1 && cx != -1 {
		task.MinZoom = cz
		task.StartCol = cx
	} else {
		task.StartCol = -1
	}
	for i := 0; i < len(layers); i++ {
		if layers[i].URL == "" {
			layers[i].URL = m.URL
		}
		layers[i].Count = GetTileCount(&layers[i].Bound, layers[i].Zoom)
		task.Total += layers[i].Count
	}
	task.workers = make(chan TileXyz, task.workerCount)
	task.savingpipe = make(chan Tile, task.savePipeSize)
	if task.outformat == "mbtiles" {
		err := task.SetupMBTileTables(id != "")
		if err != nil {
			log.Errorf("Database connect and prepare error")
			return nil, err
		}
	}
	if task.outformat == "mysql" {
		err := task.SetupMysqlTables(id != "")
		if err != nil {
			log.Errorf("Database connect and prepare error")
			return nil, err
		}
	}
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = task.workerCount
	http.DefaultTransport.(*http.Transport).MaxConnsPerHost = task.workerCount
	http.DefaultTransport.(*http.Transport).IdleConnTimeout = time.Second * 5
	http.DefaultTransport.(*http.Transport).MaxIdleConns = task.workerCount
	http.DefaultClient.Timeout = time.Minute * 5
	return &task, nil
}

//MetaItems 输出
func (task *Task) MetaItems() map[string]string {
	b := task.Layers[len(task.Layers)-1].Bound
	x := (b.East - b.West) / 2
	y := (b.South - b.North) / 2
	data := map[string]string{
		"id":          task.ID,
		"name":        task.Name,
		"description": task.Description,
		"attribution": `<a href="http://www.atlasdata.cn/" target="_blank">&copy; MapCloud</a>`,
		"basename":    task.TileMap.Name,
		"format":      task.TileMap.Format,
		"type":        task.TileMap.Schema,
		"pixel_scale": strconv.Itoa(256),
		"version":     MBTileVersion,
		"bounds":      fmt.Sprintf(`%f,%f,%f,%f`, b.West, b.South, b.East, b.North),
		"center":      fmt.Sprintf(`%f,%f,%d`, x, y, (task.MinZoom+task.MaxZoom)/2),
		"minzoom":     strconv.Itoa(task.MinZoom),
		"maxzoom":     strconv.Itoa(task.MaxZoom),
		"json":        task.TileMap.Bound,
	}
	return data
}

func (task *Task) SetupMBTileTables(ignore bool) error {
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
	if !ignore {
		_, _ = db.Exec("create unique index name on metadata (name);")
		_, _ = db.Exec("create unique index tile_index on tiles(zoom_level, tile_column, tile_row);")
		// Load metadata.
		for name, value := range task.MetaItems() {
			_, err := db.Exec("insert or ignore into metadata (name, value) values (?, ?)", name, value)
			if err != nil {
				return err
			}
		}
	}

	task.db = db //保存任务的库连接
	return nil
}

func (task *Task) SetupMysqlTables(ignore bool) error {
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
	if !ignore {
		_, _ = db.Exec("create unique index name on metadata (name);")
		_, _ = db.Exec("create unique  index tile_index on tiles(zoom_level, tile_column, tile_row);")
		// Load metadata.
		for name, value := range task.MetaItems() {
			_, err := db.Exec("insert ignore into metadata (name, value) values (?, ?)", name, value)
			if err != nil {
				return err
			}
		}
	}
	task.db = db //保存任务的库连接
	return nil
}

func (task *Task) abortFun() {
	task.abort <- struct{}{}
	task.signal = Aborting
	go func() {
		for true {
			if task.signal == Terminated {
				task.saveCursor()
				_ = task.redisPool.Close()
				_ = task.db.Close()
				break
			}
			time.Sleep(time.Second * 2)
		}
	}()
}

func (task *Task) pauseFun() {
	task.pause <- struct{}{}
	task.signal = Pause
}

func (task *Task) playFun() {
	task.play <- struct{}{}
	task.signal = Running
}

//SavePipe 保存瓦片管道
func (task *Task) savePipe() {
	var batch []Tile
	for tile := range task.savingpipe {
		batch = append(batch, tile)
		if len(batch) == task.savePipeSize {
			err := saveToMBTile(batch, task.db, task.outformat)
			if err != nil {
				task.saveFailedToRedis(batch)
				log.Errorf("save tile to mbtiles db error ~ %s", err)
			}
			batch = []Tile{}
		}
	}
	if task.signal < Terminated {
		err := saveToMBTile(batch, task.db, task.outformat)
		if err != nil {
			task.saveFailedToRedis(batch)
			log.Errorf("save tile to mbtiles db error ~ %s", err)
		} else {
			log.Infof("save batch complete count %d", len(batch))
		}
		batch = []Tile{}
	}
	task.wg.Done()
	task.signal = Terminated
}

//SaveTile 保存瓦片
func (task *Task) saveTile(tile Tile, format string) error {
	defer task.wg.Done()
	err := saveToFiles(tile, filepath.Base(task.File), format)
	if err != nil {
		log.Errorf("create %v tile file error ~ %s", tile, err)
	}
	return nil
}

//tileFetcher 瓦片加载器
func (task *Task) tileFetcher(t TileXyz, url string, isRetry bool) {
	defer func() {
		task.wg.Done()
		<-task.workers
	}()
	prep := func(t TileXyz, url string) string {
		url = strings.Replace(url, "{x}", strconv.Itoa(t.X), -1)
		url = strings.Replace(url, "{y}", strconv.Itoa(t.Y), -1)
		url = strings.Replace(url, "{z}", strconv.Itoa(t.Z), -1)
		return url
	}
	pbf := prep(t, url)
	resp, err := http.Get(pbf)
	if err != nil {
		task.errToRedis(t, err.Error())
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
			log.Errorf("fetch %v tile error, status code: %d ~", t, resp.StatusCode)
		} else if resp.StatusCode == 404 && isRetry {
			task.cleanFail(t)
		}
		task.errToRedis(t, "resp "+strconv.Itoa(resp.StatusCode))
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		task.errToRedis(t, err.Error())
		log.Errorf("read %v tile error ~ %s", t, err)
		return
	}
	if len(body) == 0 {
		if isRetry {
			task.cleanFail(t)
		}
		task.errToRedis(t, "nil tile")
		return //zero byte tiles n
	}
	tile := Tile{X: t.X, Y: t.Y, Z: t.Z, C: body}
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
	if task.outformat == "mbtiles" || task.outformat == "mysql" {
		if task.signal < Aborting {
			task.savingpipe <- tile
		}
	} else {
		task.wg.Add(1)
		_ = task.saveTile(tile, task.TileMap.Format)
	}
	if isRetry {
		task.cleanFail(t)
	}
}

//DownloadZoom 下载指定层级
func (task *Task) downloadLayer(layer TileOption) {
	bar := pb.New64(int64(layer.Count)).Prefix(fmt.Sprintf("Zoom %d : ", layer.Zoom))
	bar.Start()
	var tileList = make(chan TileXyz, 0)
	go GenerateTiles(&GenerateTilesOptions{
		Bounds:   &layer.Bound,
		Zoom:     layer.Zoom,
		Consumer: tileList,
	})

	for tile := range tileList {
		if task.StartCol != -1 && layer.Zoom == task.MinZoom {
			if tile.X < task.StartCol-1 {
				bar.Increment()
				continue
			}
		}
		if task.CurCol != tile.X {
			task.CurCol = tile.X
			task.saveCursor()
		}
		count := bar.Get()
		if count > 0 && count%10000000 == 0 {
			time.Sleep(time.Minute * 2)
		}
		select {
		case task.workers <- tile:
			bar.Increment()
			task.wg.Add(1)
			go task.tileFetcher(tile, layer.URL, false)
		case <-task.abort:
			close(tileList)
			log.Infof("task %s got canceled.", task.ID)
		case <-task.pause:
			bar.Increment()
			task.wg.Add(1)
			go task.tileFetcher(tile, layer.URL, false)
			log.Infof("task %s suspended.", task.ID)
			select {
			case <-task.play:
				log.Infof("task %s go on.", task.ID)
			case <-task.abort:
				close(tileList)
				log.Infof("task %s got canceled.", task.ID)
			}
		}
	}
	task.wg.Wait()
	bar.FinishPrint(fmt.Sprintf("Task %s zoom %d finished ~", task.ID, layer.Zoom))
}

//Download 开启下载任务
func (task *Task) Download() {
	task.signal = Running
	go task.savePipe()
	go task.printPipe()
	go task.retryLoop()
	for _, layer := range task.Layers {
		if layer.Zoom >= task.MinZoom && task.signal < Pause {
			task.CurZoom = layer.Zoom
			task.downloadLayer(layer)
		}
	}
	task.signal = Ending
	task.wg.Add(1)
	close(task.savingpipe)
	task.wg.Wait()
	task.signal = Terminated
	log.Infof("task %s finished ~", task.ID)
}
func (task *Task) printPipe() {
	for true {
		if task.signal == Terminated {
			break
		}
		time.Sleep(time.Second * 5)
		log.Debugf("cache pipe size %d", len(task.savingpipe))
	}
}
func (task *Task) retryLoop() {
	for true {
		task.retry()
		time.Sleep(time.Second * 5)
		if task.signal == Terminated {
			break
		}
	}
}
