package stress

import (
	"bytes"
	"compress/gzip"
	"io"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/write"
)

// WriteResult contains the latency, status code, and error type
// each time a write happens.
type WriteResult struct {
	LatNs      int64
	StatusCode int
	Body       string // Only populated when unusual status code encountered.
	Err        error
}

// WriteConfig specifies the configuration for the Write function.
type WriteConfig struct {
	BatchSize uint64
	MaxPoints uint64

	// If 0 (NoCompression), do not gzip at all.
	// Otherwise, pass this value to the gzip writer.
	GzipLevel int

	Deadline time.Time
	Tick     <-chan time.Time
	Results  chan<- WriteResult
}

// Write takes in a slice of lineprotocol.Points, a write.Client, and a WriteConfig. It will attempt
// to write data to the target until one of the following conditions is met.
// 1. We reach that MaxPoints specified in the WriteConfig.
// 2. We've passed the Deadline specified in the WriteConfig.
func Write(pc <-chan []lineprotocol.Point, c write.Client, cfg WriteConfig) (uint64, time.Duration) {
	if cfg.Results == nil {
		panic("Results Channel on WriteConfig cannot be nil")
	}
	var pointCount uint64

	start := time.Now()
	buf := bytes.NewBuffer(nil)
	t := time.Now()

	var w io.Writer = buf

	doGzip := cfg.GzipLevel != 0
	var gzw *gzip.Writer
	if doGzip {
		var err error
		gzw, err = gzip.NewWriterLevel(w, cfg.GzipLevel)
		if err != nil {
			// Should only happen with an invalid gzip level?
			panic(err)
		}
		w = gzw
	}

WRITE_BATCHES:
	for {
		if t.After(cfg.Deadline) {
			break WRITE_BATCHES
		}

		if pointCount >= cfg.MaxPoints {
			break
		}

		// Get next batch of points from the generator.
		pts := <-pc
		for _, pt := range pts {
			pointCount++
			pt.SetTime(t)
			lineprotocol.WritePoint(w, pt)
			if pointCount%cfg.BatchSize == 0 {
				if doGzip {
					// Must Close, not Flush, to write full gzip content to underlying bytes buffer.
					if err := gzw.Close(); err != nil {
						panic(err)
					}
				}
				sendBatch(c, buf, cfg.Results)
				if doGzip {
					// sendBatch already reset the bytes buffer.
					// Reset the gzip writer to start clean.
					gzw.Reset(buf)
				}

				t = <-cfg.Tick
				if t.After(cfg.Deadline) {
					break WRITE_BATCHES
				}

				if pointCount >= cfg.MaxPoints {
					break
				}

			}
			pt.Update()
		}
	}

	return pointCount, time.Since(start)
}

func sendBatch(c write.Client, buf *bytes.Buffer, ch chan<- WriteResult) {
	lat, status, body, err := c.Send(buf.Bytes())
	buf.Reset()
	select {
	case ch <- WriteResult{LatNs: lat, StatusCode: status, Body: body, Err: err}:
	default:
	}
}
