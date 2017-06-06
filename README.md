# Stress tool

## Build Instructions
```sh
go install ./cmd/... ./lineprotocol/... ./point/... ./stress/... ./write/...
```

## Top Level Command
```
Create artificial load on an InfluxDB instance

Usage:
  influx_stress [command]

  Available Commands:
    insert      Insert data into InfluxDB

    Flags:
      -h, --help   help for influx_stress

      Use "influx_stress [command] --help" for more information about a command.
```

## Insert Subcommand
```bash
Insert data into InfluxDB

Usage:
  influx_stress insert SERIES FIELDS [flags]

Flags:
  -b, --batch-size uint      number of points in a batch (default 10000)
  -c, --consistency string   Write consistency (only applicable to clusters) (default "one")
      --create string        Use a custom create database command
      --db string            Database that will be written to (default "stress")
      --dump string          Dump to given file instead of writing over HTTP
  -f, --fast                 Run as fast as possible
      --gzip int             If non-zero, gzip write bodies with given compression level. 1=best speed, 9=best compression, -1=gzip default.
      --host string          Address of InfluxDB instance (default "http://localhost:8086")
      --pass string          Password for user
  -n, --points uint          number of points that will be written (default 18446744073709551615)
      --pps uint             Points Per Second (default 200000)
  -p, --precision string     Resolution of data being written (default "n")
  -q, --quiet                Only print the write throughput
      --rp string            Retention Policy that will be written to
  -r, --runtime duration     Total time that the test will run (default 2562047h47m16.854775807s)
  -s, --series int           number of series that will be written (default 100000)
      --strict               Strict mode will exit as soon as an error or unexpected status is encountered
      --user string          User to write data as
```

## Example Usage

Runs forever
```bash
$ influx_stress insert
```

Runs forever writing as fast as possible
```bash
$ influx_stress insert -f
```

Runs for 1 minute writing as fast as possible
```bash
$ influx_stress insert -r 1m -f
```

Writing an example series key
```bash
$ influx_stress insert cpu,host=server,location=us-west,id=myid
```

Writing an example series key with 20,000 series
```bash
$ influx_stress insert -s 20000 cpu,host=server,location=us-west,id=myid
```

Writing an example point
```bash
$ influx_stress insert cpu,host=server,location=us-west,id=myid busy=100,idle=10,random=5i
```
