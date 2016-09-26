# Stress tool

```bash
$ influx-stress write -h
Write data

Usage:
  influx-stress write SERIES FIELDS [flags]

Flags:
  -b, --batch-size int     number of points in a batch (default 10000)
      --db string          Database that will be written to (default "stress")
  -f, --fast               Run as fast as possible
      --host string        Address of InfluxDB instance (default "http://localhost:8086")
  -n, --points int         number of points that will be written (default 9223372036854775807)
      --pps int            Points Per Second (default 200000)
  -p, --precision string   Resolution of data being written (default "n")
      --rp string          Retention Policy that will be written to
  -r, --runtime duration   Total time that the test will run (default 2562047h47m16.854775807s)
  -s, --series int         number of series that will be written (default 100000)
```

## Example Usage

Runs forever
```bash
$ influx-stress write
```

Runs forever writing as fast as possible
```bash
$ influx-stress write -f
```

Runs for 1 minute writing as fast as possible
```bash
$ influx-stress write -r 1m -f
```

Writing an example series key
```bash
$ influx-stress write cpu,host=server,location=us-west,id=myid
```

Writing an example series key with 20,000 series
```bash
$ influx-stress write -s 20000 cpu,host=server,location=us-west,id=myid
```

Writing an example point
```bash
$ influx-stress write cpu,host=server,location=us-west,id=myid busy=100,idle=10,random=5i
```
