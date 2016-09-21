package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/stress"
	"github.com/influxdata/influx-stress/write"
)

type point struct {
	seriesKey []byte
	N         *lineprotocol.Int
	time      *lineprotocol.Timestamp
	fields    []lineprotocol.Field
}

func newPoint(sk []byte, p lineprotocol.Precision) *point {
	fields := make([]lineprotocol.Field, 1)
	e := &point{
		seriesKey: sk,
		time:      lineprotocol.NewTimestamp(p),
		fields:    fields,
	}

	n := &lineprotocol.Int{Key: []byte("n")}
	e.N = n
	e.fields[0] = n

	return e
}

func (p *point) Series() []byte {
	return p.seriesKey
}

func (p *point) Fields() []lineprotocol.Field {
	return p.fields
}

func (p *point) Time() *lineprotocol.Timestamp {
	return p.time
}

func (p *point) SetTime(t time.Time) {
	p.time.Time = t
}

func (p *point) Update() {
	atomic.AddInt64(&p.N.Value, int64(1))
}

func style(tmpl string) []lineprotocol.Point {
	pts := make([]lineprotocol.Point, 0, 100000)
	for i := 0; i < 1000; i++ {
		for j := 0; j < 100; j++ {
			pt := newPoint([]byte(fmt.Sprintf(tmpl, i, j)), lineprotocol.Nanosecond)
			pts = append(pts, pt)
		}
	}
	return pts
}

func test(tmpl, addr string) {
	http.Get(fmt.Sprintf("http://%v:8086/query?q=create+database+stress", addr))
	c := write.NewClient(fmt.Sprintf("http://%v:8086", addr), "stress", "", "n")

	stdPts := style(tmpl)
	tick := time.Tick(time.Nanosecond)

	cfg := stress.WriteConfig{
		BatchSize: 10000,
		MaxPoints: 100000000,
		Deadline:  time.Now().Add(1 * time.Minute),
		Tick:      tick,
		Results:   make(chan stress.WriteResult, 0),
	}

	pointsWritten, duration := stress.Write(stdPts, c, cfg)

	fmt.Println(float64(pointsWritten) / duration.Seconds())
}

func main() {
	_ = "localhost"
	test("c3,a=%v,b=%v", host)

	time.Sleep(10 * time.Second)
}
