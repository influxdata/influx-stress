package cmd

import (
	"fmt"
	"math"
	"math/rand"
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
	dbsN int
)

var insertManyDBsCmd = &cobra.Command{
	Use:   "insert-many-dbs SERIES FIELDS",
	Short: "Insert data into InfluxDB for many databases", // better descriiption
	Long:  "",
	Run:   insertManyDBsRun,
}

func insertManyDBsRun(cmd *cobra.Command, args []string) {
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

	pps = pps / uint64(dbsN)
	batchSize = batchSize / uint64(dbsN)
	seriesN = seriesN / dbsN

	concurrency := pps / batchSize
	if !quiet {
		fmt.Printf("Running %v stresses with the following config:\n", dbsN)
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

	var wgDB sync.WaitGroup
	// Start Number of DBs here
	for i := 0; i < dbsN; i++ {

		wgDB.Add(1)
		go func(i int) {
			c := dbclient(fmt.Sprintf("%v%v", db, i))
			manyDBsCommand := fmt.Sprintf("CREATE DATABASE %v%v WITH SHARD DURATION 1h", db, i)

			r := rand.Intn(500)
			time.Sleep(time.Millisecond * time.Duration(r))
			//time.Sleep(time.Millisecond * time.Duration(r))
			if err := c.Create(manyDBsCommand); err != nil {
				fmt.Fprintln(os.Stderr, "Failed to create database:", err.Error())
				fmt.Fprintln(os.Stderr, "Aborting.")
				return
			}
			r = rand.Intn(500)
			time.Sleep(time.Second * 10)

			pts := point.NewPoints(seriesKey, fieldStr, seriesN, lineprotocol.Nanosecond)

			startSplit := 0
			inc := int(seriesN) / int(concurrency)
			endSplit := inc

			sink := newResultSink(int(concurrency))

			var wg sync.WaitGroup
			wg.Add(int(concurrency))

			var totalWritten uint64

			r = rand.Intn(500)
			time.Sleep(time.Millisecond * time.Duration(r))
			start := time.Now()
			for i := uint64(0); i < concurrency; i++ {

				go func(startSplit, endSplit int) {
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
					pointsWritten, _ := stress.Write(pts[startSplit:endSplit], c, cfg)
					atomic.AddUint64(&totalWritten, pointsWritten)

					wg.Done()
				}(startSplit, endSplit)

				startSplit = endSplit
				endSplit += inc
			}

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

			wgDB.Done()
		}(i)
	}

	wgDB.Wait()
}

func init() {
	RootCmd.AddCommand(insertManyDBsCmd)
	insertManyDBsCmd.Flags().IntVarP(&dbsN, "dbs", "", 10, "number of databases that will be written")

	insertManyDBsCmd.Flags().StringVarP(&host, "host", "", "http://localhost:8086", "Address of InfluxDB instance")
	insertManyDBsCmd.Flags().StringVarP(&username, "user", "", "", "User to write data as")
	insertManyDBsCmd.Flags().StringVarP(&password, "pass", "", "", "Password for user")
	insertManyDBsCmd.Flags().StringVarP(&db, "db", "", "stress", "Database that will be written to")
	insertManyDBsCmd.Flags().StringVarP(&rp, "rp", "", "", "Retention Policy that will be written to")
	insertManyDBsCmd.Flags().StringVarP(&precision, "precision", "p", "n", "Resolution of data being written")
	insertManyDBsCmd.Flags().StringVarP(&consistency, "consistency", "c", "one", "Write consistency (only applicable to clusters)")
	insertManyDBsCmd.Flags().IntVarP(&seriesN, "series", "s", 100000, "number of series that will be written")
	insertManyDBsCmd.Flags().Uint64VarP(&pointsN, "points", "n", math.MaxUint64, "number of points that will be written")
	insertManyDBsCmd.Flags().Uint64VarP(&batchSize, "batch-size", "b", 10000, "number of points in a batch")
	insertManyDBsCmd.Flags().Uint64VarP(&pps, "pps", "", 200000, "Points Per Second")
	insertManyDBsCmd.Flags().DurationVarP(&runtime, "runtime", "r", time.Duration(math.MaxInt64), "Total time that the test will run")
	insertManyDBsCmd.Flags().BoolVarP(&fast, "fast", "f", false, "Run as fast as possible")
	insertManyDBsCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Only print the write throughput")
	insertManyDBsCmd.Flags().StringVar(&createCommand, "create", "", "Use a custom create database command")
	insertManyDBsCmd.Flags().IntVar(&gzip, "gzip", 0, "If non-zero, gzip write bodies with given compression level. 1=best speed, 9=best compression, -1=gzip default.")
	insertManyDBsCmd.Flags().StringVar(&dump, "dump", "", "Dump to given file instead of writing over HTTP")
	insertManyDBsCmd.Flags().BoolVarP(&strict, "strict", "", false, "Strict mode will exit as soon as an error or unexpected status is encountered")
}

func dbclient(d string) write.Client {
	cfg := write.ClientConfig{
		BaseURL:         host,
		Database:        d,
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
