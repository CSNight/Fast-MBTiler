package main

import (
	"database/sql"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func saveToMBTile(tiles []Tile, db *sql.DB, dt string) error {
	if dt == "mysql" {
		return saveToMysql(tiles, db)
	}
	tx, er := db.Begin()
	if er != nil {
		return er
	}
	sqlStr := "insert or ignore into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);"
	for _, tile := range tiles {
		_, err := tx.Exec(sqlStr, tile.T.Z, tile.T.X, tile.flipY(), tile.C)
		if err != nil {
			return err
		}
	}
	err := tx.Commit()
	time.Sleep(time.Microsecond * 50)
	if err != nil {
		return err
	}
	return nil
}

func saveToMysql(tiles []Tile, db *sql.DB) error {
	sqlStr := "insert ignore into tiles (zoom_level, tile_column, tile_row, tile_data) values %s"
	placeholder := "(?,?,?,?)"
	bulkValues := []interface{}{}
	valueStrings := make([]string, 0)
	for _, tile := range tiles {
		valueStrings = append(valueStrings, placeholder)
		bulkValues = append(bulkValues, tile.T.Z, tile.T.X, tile.flipY(), tile.C)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmStr := fmt.Sprintf(sqlStr, strings.Join(valueStrings, ","))
	res, err := tx.Exec(stmStr, bulkValues...)
	err = tx.Commit()
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	log.Infof("save batch count %d,insert %d", len(tiles), rows)
	if err != nil {
		return err
	}
	return nil
}
func saveToFiles(tile Tile, rootdir string) error {
	dir := filepath.Join(rootdir, fmt.Sprintf(`%d`, tile.T.Z), fmt.Sprintf(`%d`, tile.T.X))
	os.MkdirAll(dir, os.ModePerm)
	fileName := filepath.Join(dir, fmt.Sprintf(`%d.png`, tile.T.Y))
	err := ioutil.WriteFile(fileName, tile.C, os.ModePerm)
	if err != nil {
		return err
	}
	log.Println(fileName)
	return nil
}

func optimizeConnection(db *sql.DB) error {
	_, err := db.Exec("PRAGMA synchronous=1")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA locking_mode=EXCLUSIVE")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA journal_mode=OFF")
	if err != nil {
		return err
	}
	return nil
}

func loadCollection(path string) orb.Collection {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	fc, err := geojson.UnmarshalFeatureCollection(data)
	if err != nil {
		log.Fatalf("unable to unmarshal feature: %v", err)
	}

	var collection orb.Collection
	for _, f := range fc.Features {
		collection = append(collection, f.Geometry)
	}

	return collection
}
