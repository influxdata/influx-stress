package main

import (
	"bytes"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/write"
)

func main() {
	c := write.NewClient("http://localhost:8086", "stress", "n")
	g := lineprotocol.NewGenerator(nil)

	buf := bytes.NewBuffer(nil)

	for i := 0; i < 200; i++ {
		g.WriteTo(buf)
	}
	c.Send(buf.Bytes())
}
