package main

import (
	"sync"

	"github.com/paulmach/orb"

	"github.com/paulmach/orb/maptile"
)

//TileSize 默认瓦片大小
const TileSize = 256

//Tile 自定义瓦片存储
type Tile struct {
	T maptile.Tile
	C []byte
}

func (tile Tile) flipY() uint32 {
	return (1 << uint32(tile.T.Z)) - tile.T.Y - 1
}

//Set a safety set
type Set struct {
	sync.RWMutex
	M maptile.Set
}

//Layer 级别&瓦片数
type Layer struct {
	URL        string
	Zoom       uint32
	Count      int64
	Collection orb.Collection
}

// Constants representing TileFormat types
const (
	GZIP string = "gzip" // encoding = gzip
	ZLIB        = "zlib" // encoding = deflate
	PNG         = "png"
	JPG         = "jpg"
	PBF         = "pbf"
	WEBP        = "webp"
)
