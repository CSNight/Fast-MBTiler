package main

import (
	"fmt"
	"math"
)

const threeSixty float64 = 360.0
const oneEighty float64 = 180.0
const radius float64 = 6378137.0
const webMercatorLatLimit float64 = 85.05112877980659

type ErrTile struct {
	X   int    `json:"x"`
	Y   int    `json:"y"`
	Z   int    `json:"z"`
	Res string `json:"res"`
}

//Tile 自定义瓦片存储
type Tile struct {
	X int
	Y int
	Z int
	C []byte
}
type TileXyz struct {
	X int
	Y int
	Z int
}

func (tile Tile) flipY() int {
	return (1 << tile.Z) - tile.Y - 1
}

type LngLatBbox struct {
	West  float64 `json:"west"`
	East  float64 `json:"east"`
	North float64 `json:"north"`
	South float64 `json:"south"`
}

//Layer 级别&瓦片数
type TileOption struct {
	URL   string
	Zoom  int
	Count int
	Bound LngLatBbox
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

type GenerateTilesOptions struct {
	Bounds   *LngLatBbox
	Zoom     int
	Consumer chan TileXyz
}

//LngLat holds a standard geographic coordinate pair in decimal degrees
type LngLat struct {
	Lng, Lat float64
}

//LngLatBbox bounding box of a tile, in decimal degrees

// Intersects returns true if this bounding box intersects with the other bounding box.
func (b *LngLatBbox) Intersects(o *LngLatBbox) bool {
	latOverlaps := (o.North > b.South) && (o.South < b.North)
	lngOverlaps := (o.East > b.West) && (o.West < b.East)
	return latOverlaps && lngOverlaps
}

//XY holds a Spherical Mercator point
type XY struct {
	X, Y float64
}

func deg2rad(deg float64) float64 {
	return deg * (math.Pi / oneEighty)
}

func rad2deg(rad float64) float64 {
	return rad * (oneEighty / math.Pi)
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetTile returns a tile for a given longitude latitude and zoom level
func GetTile(lng float64, lat float64, zoom int) *TileXyz {

	latRad := deg2rad(lat)
	n := math.Pow(2.0, float64(zoom))
	x := int(math.Floor((lng + oneEighty) / threeSixty * n))
	y := int(math.Floor((1.0 - math.Log(math.Tan(latRad)+(1.0/math.Cos(latRad)))/math.Pi) / 2.0 * n))

	return &TileXyz{x, y, zoom}

}

func GetTileCount(bounds *LngLatBbox, zoom int) int {
	var boxes []*LngLatBbox
	if bounds.West > bounds.East {
		boxes = []*LngLatBbox{
			{-180.0, bounds.South, bounds.East, bounds.North},
			{bounds.West, bounds.South, 180.0, bounds.North},
		}
	} else {
		boxes = []*LngLatBbox{bounds}
	}
	var count int
	for _, box := range boxes {
		// Clamp the individual boxes to web mercator limits
		clampedBox := &LngLatBbox{
			West:  math.Max(-180.0, box.West),
			South: math.Max(-webMercatorLatLimit, box.South),
			East:  math.Min(180.0, box.East),
			North: math.Min(webMercatorLatLimit, box.North),
		}
		ll := GetTile(clampedBox.West, clampedBox.South, zoom)
		ur := GetTile(clampedBox.East, clampedBox.North, zoom)
		llx := ll.X
		if llx < 0 {
			llx = 0
		}
		ury := ur.Y
		if ury < 0 {
			ury = 0
		}
		row := min(ur.X+1, 1<<zoom)
		column := min(ll.Y+1, 1<<zoom)
		count += (column - llx) * (row - ury)
	}
	return count
}

func GenerateTiles(opts *GenerateTilesOptions) {
	bounds := opts.Bounds
	zoom := opts.Zoom
	consumer := opts.Consumer
	complete := false
	defer func() {
		if complete {
			close(consumer)
		}
	}()
	defer func() {
		if recover() != nil {

		}
	}()
	var boxes []*LngLatBbox
	if bounds.West > bounds.East {
		boxes = []*LngLatBbox{
			{-180.0, bounds.South, bounds.East, bounds.North},
			{bounds.West, bounds.South, 180.0, bounds.North},
		}
	} else {
		boxes = []*LngLatBbox{bounds}
	}

	for _, box := range boxes {
		// Clamp the individual boxes to web mercator limits
		clampedBox := &LngLatBbox{
			West:  math.Max(-180.0, box.West),
			South: math.Max(-webMercatorLatLimit, box.South),
			East:  math.Min(180.0, box.East),
			North: math.Min(webMercatorLatLimit, box.North),
		}

		ll := GetTile(clampedBox.West, clampedBox.South, zoom)
		ur := GetTile(clampedBox.East, clampedBox.North, zoom)

		llx := ll.X
		if llx < 0 {
			llx = 0
		}

		ury := ur.Y
		if ury < 0 {
			ury = 0
		}
		row := min(ur.X+1, 1<<zoom)
		column := min(ll.Y+1, 1<<zoom)
		for i := llx; i < column; i++ {
			for j := ury; j < row; j++ {
				x := i
				y := j
				consumer <- TileXyz{Z: zoom, X: x, Y: y}
			}
		}
	}
	complete = true
}

// Equals compares 2 tiles
func (tile *TileXyz) Equals(t2 *Tile) bool {
	return tile.X == t2.X && tile.Y == t2.Y && tile.Z == t2.Z
}

//Ul returns the upper left corner of the tile decimal degrees
func (tile *TileXyz) Ul() *LngLat {
	n := math.Pow(2.0, float64(tile.Z))
	lonDeg := float64(tile.X)/n*threeSixty - oneEighty
	latRad := math.Atan(math.Sinh(math.Pi * (1 - (2 * float64(tile.Y) / n))))
	latDeg := rad2deg(latRad)
	return &LngLat{lonDeg, latDeg}
}

//Bounds returns a LngLatBbox for a given tile
func (tile *TileXyz) Bounds() *LngLatBbox {
	a := tile.Ul()
	shifted := TileXyz{tile.X + 1, tile.Y + 1, tile.Z}
	b := shifted.Ul()
	return &LngLatBbox{a.Lng, b.Lat, b.Lng, a.Lat}
}

func (tile *TileXyz) Parent() *TileXyz {

	if tile.Z == 0 && tile.X == 0 && tile.Y == 0 {
		return tile
	}

	if math.Mod(float64(tile.X), 2) == 0 && math.Mod(float64(tile.Y), 2) == 0 {
		return &TileXyz{tile.X / 2, tile.Y / 2, tile.Z - 1}
	}
	if math.Mod(float64(tile.X), 2) == 0 {
		return &TileXyz{tile.X / 2, (tile.Y - 1) / 2, tile.Z - 1}
	}
	if math.Mod(float64(tile.X), 2) != 0 && math.Mod(float64(tile.Y), 2) != 0 {
		return &TileXyz{(tile.X - 1) / 2, (tile.Y - 1) / 2, tile.Z - 1}
	}
	if math.Mod(float64(tile.X), 2) != 0 && math.Mod(float64(tile.Y), 2) == 0 {
		return &TileXyz{(tile.X - 1) / 2, tile.Y / 2, tile.Z - 1}
	}
	return nil
}

func (tile *TileXyz) Children() []*TileXyz {

	kids := []*TileXyz{
		{tile.X * 2, tile.Y * 2, tile.Z + 1},
		{tile.X*2 + 1, tile.Y * 2, tile.Z + 1},
		{tile.X*2 + 1, tile.Y*2 + 1, tile.Z + 1},
		{tile.X * 2, tile.Y*2 + 1, tile.Z + 1},
	}
	return kids
}

// ToString returns a string representation of the tile.
func (tile *TileXyz) ToString() string {
	return fmt.Sprintf("{%d/%d/%d}", tile.Z, tile.X, tile.Y)
}

//ToXY transforms WGS84 DD to Spherical Mercator meters
func ToXY(ll *LngLat) *XY {

	x := radius * deg2rad(ll.Lng)
	intrx := (math.Pi * 0.25) + (0.5 * deg2rad(ll.Lat))
	y := radius * math.Log(math.Tan(intrx))

	return &XY{x, y}
}
