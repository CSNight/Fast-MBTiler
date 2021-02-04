package main

import (
	"database/sql"
	"fmt"
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
	start := time.Now()
	tx, er := db.Begin()
	if er != nil {
		return er
	}
	sqlStr := "insert or ignore into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);"
	for _, tile := range tiles {
		_, err := tx.Exec(sqlStr, tile.Z, tile.X, tile.flipY(), tile.C)
		if err != nil {
			return err
		}
	}
	err := tx.Commit()
	secs := time.Since(start).Milliseconds()
	log.Infof("save batch count %d,cost %d", len(tiles), secs)
	time.Sleep(time.Microsecond * 50)
	if err != nil {
		return err
	}
	return nil
}

func saveToMysql(tiles []Tile, db *sql.DB) error {
	start := time.Now()
	sqlStr := "insert ignore into tiles (zoom_level, tile_column, tile_row, tile_data) values %s"
	placeholder := "(?,?,?,?)"
	bulkValues := []interface{}{}
	valueStrings := make([]string, 0)
	for _, tile := range tiles {
		valueStrings = append(valueStrings, placeholder)
		bulkValues = append(bulkValues, tile.Z, tile.X, tile.flipY(), tile.C)
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
	secs := time.Since(start).Milliseconds()
	log.Infof("save batch count %d,insert %d,cost %d", len(tiles), rows, secs)
	if err != nil {
		return err
	}
	return nil
}
func saveToFiles(tile Tile, rootdir string, format string) error {
	dir := filepath.Join(rootdir, fmt.Sprintf(`%d`, tile.Z), fmt.Sprintf(`%d`, tile.X))
	os.MkdirAll(dir, os.ModePerm)
	fileName := filepath.Join(dir, fmt.Sprintf(`%d.`+format, tile.Y))
	err := ioutil.WriteFile(fileName, tile.C, os.ModePerm)
	if err != nil {
		return err
	}
	log.Println(fileName)
	return nil
}

func optimizeConnection(db *sql.DB) error {
	_, err := db.Exec("PRAGMA synchronous=OFF")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA cache_size=8000")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA page_size=4096")
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
