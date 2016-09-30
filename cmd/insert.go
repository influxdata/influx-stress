package cmd

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/point"
	"github.com/influxdata/influx-stress/stress"
	"github.com/influxdata/influx-stress/write"
	"github.com/spf13/cobra"
)

var (
	host, db, rp, precision string
	seriesN                 int
	batchSize, pointsN, pps uint64
	runtime                 time.Duration
	fast                    bool
)

const (
	defaultSeriesKey string = "ctr,some=tag"
	defaultFieldStr  string = "n=0i"
)

var insertCmd = &cobra.Command{
	Use:   "insert SERIES FIELDS",
	Short: "Insert data into InfluxDB", // better descriiption
	Long:  "",
	Run:   insertRun,
}

func insertRun(cmd *cobra.Command, args []string) {
	seriesKey := defaultSeriesKey
	fieldStr := defaultFieldStr
	if len(args) >= 1 {
		seriesKey = args[0]
	}
	if len(args) == 2 {
		fieldStr = args[1]
	}

	// create databse
	http.Get(fmt.Sprintf("%v/query?q=create+database+%v", host, db))

	c := write.NewClient(host, db, rp, precision)

	pts := point.NewPoints(seriesKey, fieldStr, seriesN, lineprotocol.Nanosecond)

	concurrency := pps / batchSize
	var wg sync.WaitGroup
	wg.Add(int(concurrency))

	var totalWritten uint64

	start := time.Now()
	for i := uint64(0); i < concurrency; i++ {
		go func() {
			tick := time.Tick(time.Second)

			if fast {
				tick = time.Tick(time.Nanosecond)
			}

			cfg := stress.WriteConfig{
				BatchSize: batchSize,
				MaxPoints: pointsN / concurrency, // divide by concurreny
				Deadline:  time.Now().Add(runtime),
				Tick:      tick,
				Results:   make(chan stress.WriteResult, 0), // make result sync
			}

			// Ignore duration from a single call to Write.
			pointsWritten, _ := stress.Write(pts, c, cfg)
			atomic.AddUint64(&totalWritten, pointsWritten)

			wg.Done()
		}()
	}

	wg.Wait()
	totalTime := time.Since(start)
	fmt.Printf("Write Throughput: %v\n", int(float64(totalWritten)/totalTime.Seconds()))
}

func init() {
	RootCmd.AddCommand(insertCmd)

	insertCmd.Flags().StringVarP(&host, "host", "", "http://localhost:8086", "Address of InfluxDB instance")
	insertCmd.Flags().StringVarP(&db, "db", "", "stress", "Database that will be written to")
	insertCmd.Flags().StringVarP(&rp, "rp", "", "", "Retention Policy that will be written to")
	insertCmd.Flags().StringVarP(&precision, "precision", "p", "n", "Resolution of data being written")
	insertCmd.Flags().IntVarP(&seriesN, "series", "s", 100000, "number of series that will be written")
	insertCmd.Flags().Uint64VarP(&pointsN, "points", "n", math.MaxUint64, "number of points that will be written")
	insertCmd.Flags().Uint64VarP(&batchSize, "batch-size", "b", 10000, "number of points in a batch")
	insertCmd.Flags().Uint64VarP(&pps, "pps", "", 200000, "Points Per Second")
	insertCmd.Flags().DurationVarP(&runtime, "runtime", "r", time.Duration(math.MaxInt64), "Total time that the test will run")
	insertCmd.Flags().BoolVarP(&fast, "fast", "f", false, "Run as fast as possible")
}
