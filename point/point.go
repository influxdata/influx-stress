package point

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
)

// The point struct implements the lineprotocol.Point interface.
type point struct {
	seriesKey []byte

	// Note here that Ints and Floats are exported so they can be modified outside
	// of the point struct
	Ints   []*lineprotocol.Int
	Floats []*lineprotocol.Float

	// The fields slice should contain exactly Ints and Floats. Having this
	// slice allows us to avoid iterating through Ints and Floats in the Fields
	// function.
	fields []lineprotocol.Field

	time *lineprotocol.Timestamp
}

// New returns a new point without setting the time field.
func New(sk []byte, ints, floats []string, p lineprotocol.Precision) *point {
	fields := []lineprotocol.Field{}
	e := &point{
		seriesKey: sk,
		time:      lineprotocol.NewTimestamp(p),
		fields:    fields,
	}

	for _, i := range ints {
		n := &lineprotocol.Int{Key: []byte(i)}
		e.Ints = append(e.Ints, n)
		e.fields = append(e.fields, n)
	}

	for _, f := range floats {
		n := &lineprotocol.Float{Key: []byte(f)}
		e.Floats = append(e.Floats, n)
		e.fields = append(e.fields, n)
	}

	return e
}

// Series returns the series key for a point.
func (p *point) Series() []byte {
	return p.seriesKey
}

// Fields returns the fields for a a point.
func (p *point) Fields() []lineprotocol.Field {
	return p.fields
}

// Time returns the timestamps for a point.
func (p *point) Time() *lineprotocol.Timestamp {
	return p.time
}

// SetTime set the t to be the timestamp for a point.
func (p *point) SetTime(t time.Time) {
	p.time.SetTime(&t)
}

// Update increments the value of all of the Int and Float
// fields by 1.
func (p *point) Update() {
	for _, i := range p.Ints {
		atomic.AddInt64(&i.Value, int64(1))
	}

	for _, f := range p.Floats {
		// Need to do something else here
		// There will be a race here
		f.Value += 1.0
	}
}

// SeriesGenerator generates series to be written with influx-stress.
type SeriesGenerator struct {
	totalCardinality   int
	currentCardinality int

	template         string
	tagCardinalities []int

	batch []string
	idx   int
}

func NewSeriesGenerator(tmplt string, cardinality int) *SeriesGenerator {
	fmt.Printf("Generating %d cardinality\n", cardinality)
	s := &SeriesGenerator{}
	s.totalCardinality = cardinality
	if cardinality < 100000 {
		s.batch = make([]string, 0, cardinality)
	} else {
		s.batch = make([]string, 0, 100000)
	}

	var numTags int
	s.template, numTags = formatTemplate(tmplt)
	s.tagCardinalities = tagCardinalityPartition(numTags, primeFactorization(cardinality))
	return s
}

// Next returns the next series to be generated.
func (s *SeriesGenerator) Next() (string, bool) {
	if s.currentCardinality == s.totalCardinality && s.idx == len(s.batch) {
		// We have generated all series and written the last batch.
		return "", false
	}

	if s.idx < len(s.batch) {
		next := s.batch[s.idx]
		s.idx++
		return next, true
	}

	// Need to generate a new batch.
	s.batch, s.idx = s.batch[:0], 1
	for i := 0; i < cap(s.batch) && s.currentCardinality < s.totalCardinality; i++ {
		s.batch = append(s.batch, fmt.Sprintf(s.template, sliceMod(s.currentCardinality, s.tagCardinalities)...))
		s.currentCardinality++
	}

	// Return the first series in the batch.
	if len(s.batch) == 0 {
		return "", false
	}
	return s.batch[0], true
}

type PointGenerator struct {
	SeriesGenerator *SeriesGenerator

	// TODO(edd) make generator.
	IntFields []string
	// TODO(edd) make generator.
	FloatFields []string

	Precision lineprotocol.Precision
	batch     []lineprotocol.Point
}

// NewPointGenerator creates a new generator for emitting points. Points are
// created in batches of 20x the number of series created in a SeriesGenerator's
// batch, up to a maximum batch of 500K points.
func NewPointGenerator(seriesKey, fields string, seriesN int, pc lineprotocol.Precision) *PointGenerator {
	p := PointGenerator{
		SeriesGenerator: NewSeriesGenerator(seriesKey, seriesN),
	}
	p.IntFields, p.FloatFields = generateFieldSet(fields)

	sz := cap(p.SeriesGenerator.batch) * 20
	if sz > 500000 {
		sz = 500000
	}
	p.batch = make([]lineprotocol.Point, 0, sz)

	return &p
}

// Next returns a batch of points. A nil batch indicates there are no more
// points in the generator.
func (p *PointGenerator) Next() []lineprotocol.Point {
	// Generate a new batch. Fill the batch with points from the series
	// generator.
	p.batch = p.batch[:0]
	for i := 0; i < cap(p.batch); i++ {
		series, ok := p.SeriesGenerator.Next()
		if !ok {
			break
		}
		p.batch = append(p.batch, New([]byte(series), p.IntFields, p.FloatFields, p.Precision))
	}
	return p.batch
}

// NewPoints returns a slice of Points of length seriesN shaped like the given seriesKey.
func NewPoints(seriesKey, fields string, seriesN int, pc lineprotocol.Precision) []lineprotocol.Point {
	pts := []lineprotocol.Point{}
	series := generateSeriesKeys(seriesKey, seriesN)
	ints, floats := generateFieldSet(fields)
	for _, sk := range series {
		p := New(sk, ints, floats, pc)
		pts = append(pts, p)
	}

	return pts
}
