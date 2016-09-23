package cmd

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
	"github.com/influxdata/influx-stress/point"
	"github.com/influxdata/influx-stress/stress"
	"github.com/influxdata/influx-stress/write"
	"github.com/spf13/cobra"
)

var (
	host, db, rp, precision          string
	seriesN, pointsN, batchSize, pps int
	runtime                          time.Duration
	fast                             bool
)

var writeCmd = &cobra.Command{
	Use:   "write",
	Short: "Write data", // better descriiption
	Long:  "",
	Run:   writeRun,
}

func writeRun(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Example point in line protocol must be specified")
		_ = cmd.Usage()
		os.Exit(1)
		return
	}

	// create databse
	http.Get(fmt.Sprintf("%v/query?q=create+database+%v", host, db))

	c := write.NewClient(host, db, rp, precision)

	pts := []lineprotocol.Point{}
	for i := 0; i < seriesN; i++ {
		p := point.New([]byte(fmt.Sprintf("cpu,host=server%v", i)), []string{"n"}, nil, lineprotocol.Nanosecond)
		pts = append(pts, p)
	}

	concurrency := pps / batchSize
	var wg sync.WaitGroup
	wg.Add(concurrency)
	var totalWritten int
	var totalTime time.Duration
	for i := 0; i < concurrency; i++ {
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

			pointsWritten, duration := stress.Write(pts, c, cfg)
			totalWritten += pointsWritten

			if totalTime < duration {
				totalTime = duration
			}
			wg.Done()
		}()
	}

	wg.Wait()
	fmt.Printf("Write Throughput: %v\n", int(float64(totalWritten)/totalTime.Seconds()))

}

func init() {
	RootCmd.AddCommand(writeCmd)

	writeCmd.Flags().StringVarP(&host, "host", "", "http://localhost:8086", "Address of InfluxDB instance")
	writeCmd.Flags().StringVarP(&db, "db", "", "stress", "Database that will be written to")
	writeCmd.Flags().StringVarP(&rp, "rp", "", "", "Retention Policy that will be written to")
	writeCmd.Flags().StringVarP(&precision, "precision", "p", "n", "Resolution of data being written")
	writeCmd.Flags().IntVarP(&seriesN, "series", "s", 100000, "number of series that will be written")
	writeCmd.Flags().IntVarP(&pointsN, "points", "n", int(math.MaxInt64), "number of points that will be written")
	writeCmd.Flags().IntVarP(&batchSize, "batch-size", "b", 10000, "number of points in a batch")
	writeCmd.Flags().IntVarP(&pps, "pps", "", 200000, "Points Per Second")
	writeCmd.Flags().DurationVarP(&runtime, "runtime", "r", time.Duration(math.MaxInt64), "Total time that the test will run")
	writeCmd.Flags().BoolVarP(&fast, "fast", "f", false, "Run as fast as possible")
}
