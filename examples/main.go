package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/write"
)

type efficientPoint struct {
	seriesKey []byte
	N         *lineprotocol.Int
	time      *lineprotocol.Timestamp
	fields    []lineprotocol.Field
}

func newEfficientPoint(sk []byte, p lineprotocol.Precision) *efficientPoint {
	fields := make([]lineprotocol.Field, 1)
	e := &efficientPoint{
		seriesKey: sk,
		time:      lineprotocol.NewTimestamp(p),
		fields:    fields,
	}

	n := &lineprotocol.Int{Key: []byte("n")}
	e.N = n
	e.fields[0] = n

	return e
}

func (p *efficientPoint) Series() []byte {
	return p.seriesKey
}

func (p *efficientPoint) Fields() []lineprotocol.Field {
	return p.fields
}

func (p *efficientPoint) Time() *lineprotocol.Timestamp {
	return p.time
}

func main() {
	c := write.NewClient("http://localhost:8086", "stress", "", "n")

	buf := bytes.NewBuffer(nil)
	var pt lineprotocol.Point
	p := newEfficientPoint([]byte("cpu,host=server1"), lineprotocol.Nanosecond)
	pt = p

	fmt.Printf("%#v", p)

	tick := time.Tick(time.Microsecond)
	for i := 0; i < 1000; i++ {
		p.time.Time = <-tick
		p.N.Value++
		lineprotocol.WritePoint(buf, pt)
	}
	c.Send(buf.Bytes())
}
