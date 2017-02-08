package cmd

import (
	"fmt"
	"math"
	"os"
	"strings"
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
	host, db, rp, precision, consistency string
	username, password                   string
	createCommand, dump                  string
	seriesN, gzip                        int
	batchSize, pointsN, pps              uint64
	runtime                              time.Duration
	fast, quiet                          bool
	strict                               bool
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
		if !strings.Contains(seriesKey, ",") && !strings.Contains(seriesKey, "=") {
			fmt.Fprintln(os.Stderr, "First positional argument must be a series key, got: ", seriesKey)
			cmd.Usage()
			os.Exit(1)
			return
		}
	}
	if len(args) == 2 {
		fieldStr = args[1]
	}

	concurrency := pps / batchSize
	if !quiet {
		fmt.Printf("Using point template: %s %s <timestamp>\n", seriesKey, fieldStr)
		fmt.Printf("Using batch size of %d line(s)\n", batchSize)
		fmt.Printf("Spreading writes across %d series\n", seriesN)
		if fast {
			fmt.Println("Output is unthrottled")
		} else {
			fmt.Printf("Throttling output to ~%d points/sec\n", pps)
		}
		fmt.Printf("Using %d concurrent writer(s)\n", concurrency)

		fmt.Printf("Running until ~%d points sent or until ~%v has elapsed\n", pointsN, runtime)
	}

	c := client()

	if err := c.Create(createCommand); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create database:", err.Error())
		fmt.Fprintln(os.Stderr, "Aborting.")
		os.Exit(1)
		return
	}

	// pts := point.NewPoints(seriesKey, fieldStr, seriesN, lineprotocol.Nanosecond)
	sink := newResultSink(int(concurrency))

	var wg sync.WaitGroup
	wg.Add(int(concurrency))

	var totalWritten uint64

	var ptsChans []chan []lineprotocol.Point

	start := time.Now()
	for i := uint64(0); i < concurrency; i++ {
		ch := make(chan []lineprotocol.Point)
		ptsChans = append(ptsChans, ch)

		go func(pc chan []lineprotocol.Point) {
			tick := time.Tick(time.Second)

			if fast {
				tick = time.Tick(time.Nanosecond)
			}

			cfg := stress.WriteConfig{
				BatchSize: batchSize,
				MaxPoints: pointsN / concurrency, // divide by concurreny
				GzipLevel: gzip,
				Deadline:  time.Now().Add(runtime),
				Tick:      tick,
				Results:   sink.Chan,
			}

			// Ignore duration from a single call to Write.
			pointsWritten, _ := stress.Write(pc, c, cfg)
			atomic.AddUint64(&totalWritten, pointsWritten)

			wg.Done()
		}(ch)
	}

	// This goroutine pulls batches from the generators and multiplexes them
	// onto each writer, so that the correct number of series/points are
	// written.
	go func() {
		generator := point.NewPointGenerator(seriesKey, fieldStr, seriesN, lineprotocol.Nanosecond)

		for {
			// Next batch of points
			pts := generator.Next()
			// Multiplex them out to all writers.
			for _, c := range ptsChans {
				if len(pts) == 0 {
					close(c)
					continue
				}
				c <- pts
			}

			if len(pts) == 0 {
				return
			}
		}
	}()

	wg.Wait()
	totalTime := time.Since(start)
	if err := c.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing client: %v\n", err.Error())
	}

	sink.Close()
	throughput := int(float64(totalWritten) / totalTime.Seconds())
	if quiet {
		fmt.Println(throughput)
	} else {
		fmt.Println("Write Throughput:", throughput)
		fmt.Println("Points Written:", totalWritten)
	}
}

func init() {
	RootCmd.AddCommand(insertCmd)

	insertCmd.Flags().StringVarP(&host, "host", "", "http://localhost:8086", "Address of InfluxDB instance")
	insertCmd.Flags().StringVarP(&username, "user", "", "", "User to write data as")
	insertCmd.Flags().StringVarP(&password, "pass", "", "", "Password for user")
	insertCmd.Flags().StringVarP(&db, "db", "", "stress", "Database that will be written to")
	insertCmd.Flags().StringVarP(&rp, "rp", "", "", "Retention Policy that will be written to")
	insertCmd.Flags().StringVarP(&precision, "precision", "p", "n", "Resolution of data being written")
	insertCmd.Flags().StringVarP(&consistency, "consistency", "c", "one", "Write consistency (only applicable to clusters)")
	insertCmd.Flags().IntVarP(&seriesN, "series", "s", 100000, "number of series that will be written")
	insertCmd.Flags().Uint64VarP(&pointsN, "points", "n", math.MaxUint64, "number of points that will be written")
	insertCmd.Flags().Uint64VarP(&batchSize, "batch-size", "b", 10000, "number of points in a batch")
	insertCmd.Flags().Uint64VarP(&pps, "pps", "", 200000, "Points Per Second")
	insertCmd.Flags().DurationVarP(&runtime, "runtime", "r", time.Duration(math.MaxInt64), "Total time that the test will run")
	insertCmd.Flags().BoolVarP(&fast, "fast", "f", false, "Run as fast as possible")
	insertCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Only print the write throughput")
	insertCmd.Flags().StringVar(&createCommand, "create", "", "Use a custom create database command")
	insertCmd.Flags().IntVar(&gzip, "gzip", 0, "If non-zero, gzip write bodies with given compression level. 1=best speed, 9=best compression, -1=gzip default.")
	insertCmd.Flags().StringVar(&dump, "dump", "", "Dump to given file instead of writing over HTTP")
	insertCmd.Flags().BoolVarP(&strict, "strict", "", false, "Strict mode will exit as soon as an error or unexpected status is encountered")
}

func client() write.Client {
	cfg := write.ClientConfig{
		BaseURL:         host,
		Database:        db,
		RetentionPolicy: rp,
		User:            username,
		Pass:            password,
		Precision:       precision,
		Consistency:     consistency,
		Gzip:            gzip != 0,
	}

	if dump != "" {
		c, err := write.NewFileClient(dump, cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error opening file:", err)
			os.Exit(1)
			return c
		}

		return c
	}
	return write.NewClient(cfg)
}

type resultSink struct {
	Chan chan stress.WriteResult

	wg sync.WaitGroup
}

func newResultSink(nWriters int) *resultSink {
	s := &resultSink{
		Chan: make(chan stress.WriteResult, 8*nWriters),
	}

	s.wg.Add(1)
	go s.checkErrors()

	return s
}

func (s *resultSink) Close() {
	close(s.Chan)
	s.wg.Wait()
}

func (s *resultSink) checkErrors() {
	defer s.wg.Done()

	const timeFormat = "[2006-01-02 15:04:05]"
	for r := range s.Chan {
		if r.Err != nil {
			fmt.Fprintln(os.Stderr, time.Now().Format(timeFormat), "Error sending write:", r.Err.Error())
			continue
		}

		if r.StatusCode != 204 {
			fmt.Fprintln(os.Stderr, time.Now().Format(timeFormat), "Unexpected write: status", r.StatusCode, ", body:", r.Body)
		}

		// If we're running in strict mode then we give up at the first error.
		if strict && (r.Err != nil || r.StatusCode != 204) {
			os.Exit(1)
		}
	}
}
