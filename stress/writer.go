package stress

import (
	"bytes"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/write"
)

type WriteResult struct {
	LatNs      int64
	StatusCode int
	Err        error
}

type WriteConfig struct {
	BatchSize uint64
	MaxPoints uint64
	Deadline  time.Time
	Tick      <-chan time.Time
	Results   chan<- WriteResult
}

func Write(pts []lineprotocol.Point, c write.Client, cfg WriteConfig) (uint64, time.Duration) {
	if cfg.Results == nil {
		panic("Results Channel on WriteConfig cannot be nil")
	}
	var pointCount uint64

	start := time.Now()
	buf := bytes.NewBuffer(nil)
	t := time.Now()

	for {
		if t.After(cfg.Deadline) {
			return pointCount, time.Since(start)
		}

		for _, pt := range pts {
			pointCount++
			pt.SetTime(t)
			lineprotocol.WritePoint(buf, pt)
			if pointCount%cfg.BatchSize == 0 {
				sendBatch(c, buf, cfg.Results)
				t = <-cfg.Tick
			}
			pt.Update()
		}

		if pointCount >= cfg.MaxPoints {
			return pointCount, time.Since(start)
		}
	}
}

func sendBatch(c write.Client, buf *bytes.Buffer, ch chan<- WriteResult) {
	lat, status, err := c.Send(buf.Bytes())
	buf.Reset()
	select {
	case ch <- WriteResult{LatNs: lat, StatusCode: status, Err: err}:
	default:
	}
}
