package main

import (
	"encoding/json"
	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

func (task *Task) cleanInfo() {
	var conn redis.Conn
	defer func() {
		task.closeRedisConn(conn)
	}()
	conn = task.redisPool.Get()
	_, _ = redis.String(conn.Do("del", "cursor:"+task.ID))
	_, _ = redis.String(conn.Do("del", "nil_list:"+task.ID))
	_, _ = redis.String(conn.Do("del", "fail_list:"+task.ID))
}
func (task *Task) getCursor() (int, int) {
	var conn redis.Conn
	defer func() {
		task.closeRedisConn(conn)
	}()
	conn = task.redisPool.Get()
	replay, err := redis.String(conn.Do("get", "cursor:"+task.ID))
	if err != nil {
		return -1, -1
	} else {
		cursor := strings.Split(replay, ":")
		zoom, err := strconv.ParseInt(cursor[0], 10, 64)
		if err != nil {
			return -1, -1
		}
		col, err := strconv.ParseInt(cursor[1], 10, 64)
		if err != nil {
			return -1, -1
		}
		return int(zoom), int(col)
	}
}
func (task *Task) saveCursor() {
	var conn redis.Conn
	defer func() {
		task.closeRedisConn(conn)
	}()
	conn = task.redisPool.Get()
	replay, err := redis.Int64(conn.Do("set", "cursor:"+task.ID, strconv.Itoa(task.CurZoom)+":"+strconv.Itoa(task.CurCol)))
	if err != nil && replay < 0 {
		log.Errorf("redis save cursor failure")
	}
}
func (task *Task) saveFailedToRedis(batch []Tile) {
	for _, tile := range batch {
		task.errToRedis(TileXyz{X: tile.X, Y: tile.Y, Z: tile.Z}, "save failure")
	}
}

func (task *Task) closeRedisConn(conn redis.Conn) {
	err := conn.Close()
	if err != nil {
		log.Errorf("redis connection close failure")
	}
}

func (task *Task) errToRedis(tile TileXyz, res string) {
	var conn redis.Conn
	defer func() {
		task.closeRedisConn(conn)
	}()
	conn = task.redisPool.Get()
	et := ErrTile{
		X:   tile.X,
		Y:   tile.Y,
		Z:   tile.Z,
		Res: res,
	}
	key := "tile_" + strconv.Itoa(et.X) + "_" + strconv.Itoa(et.Y) + "_" + strconv.Itoa(et.Z)
	val, _ := json.Marshal(et)
	if res == "nil tile" || res == "resp 404" {
		replay, err := redis.Int64(conn.Do("hset", "nil_list:"+task.ID, key, val))
		if err != nil && replay < 0 {
			log.Errorf("redis save tile failure")
		}
	} else {
		replay, err := redis.Int64(conn.Do("hset", "fail_list:"+task.ID, key, val))
		if err != nil && replay < 0 {
			log.Errorf("redis save tile failure")
		}
	}
}
func (task *Task) cleanFail(t TileXyz) {
	var conn redis.Conn
	defer func() {
		task.closeRedisConn(conn)
	}()
	conn = task.redisPool.Get()
	key := "tile_" + strconv.Itoa(t.X) + "_" + strconv.Itoa(t.Y) + "_" + strconv.Itoa(t.Z)
	_, err := redis.Int64(conn.Do("hdel", "fail_list:"+task.ID, key))
	if err != nil {
		return
	}
}
func (task *Task) retry() {
	var conn redis.Conn
	var alls map[string]string
	defer func() {
		task.closeRedisConn(conn)
	}()
	conn = task.redisPool.Get()
	alls, err := redis.StringMap(conn.Do("hgetall", "fail_list:"+task.ID))
	if err != nil {
		return
	}
	for kv := range alls {
		var te ErrTile
		err = json.Unmarshal([]byte(alls[kv]), &te)
		if err != nil {
			continue
		}
		tile := TileXyz{
			X: te.X,
			Y: te.Y,
			Z: te.Z,
		}
		select {
		case task.workers <- tile:
			task.wg.Add(1)
			go task.tileFetcher(tile, task.TileMap.URL, true)
		}
	}
}
