package main

import (
	"bytes"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/write"
)

// Make example use cases

func main() {
	c := write.NewClient("http://localhost:8086", "stress", "", "n")

	buf := bytes.NewBuffer(nil)
	var pt lineprotocol.Point
	p := lineprotocol.NewPoint()
	pt = p

	for i := 0; i < 200; i++ {
		lineprotocol.WritePoint(buf, pt)
	}

	c.Send(buf.Bytes())
}
