package lineprotocol

import (
	"io"
	"time"
)

type Point interface {
	Series() []byte
	Fields() []Field
	Time() *Timestamp
	SetTime(time.Time)
	Update()
}

func WritePoint(w io.Writer, p Point) (err error) {

	_, err = w.Write(p.Series())
	if err != nil {
		return
	}

	_, err = w.Write([]byte(" "))
	if err != nil {
		return
	}

	for i, f := range p.Fields() {
		if i != 0 {
			_, err = w.Write([]byte(","))
			if err != nil {
				return
			}
		}
		_, err = f.WriteTo(w)
		if err != nil {
			return
		}
	}

	_, err = w.Write([]byte(" "))
	if err != nil {
		return
	}

	_, err = p.Time().WriteTo(w)
	if err != nil {
		return
	}

	_, err = w.Write([]byte("\n"))
	if err != nil {
		return
	}

	return
}

type point struct {
	ctr    int64
	fields []Field
	time   *Timestamp
}

func NewPoint() *point {
	i := &Int{
		Key:   []byte("value"),
		Value: int64(100),
	}
	ts := NewTimestamp(Nanosecond)
	ts.Time = time.Now()
	return &point{
		fields: []Field{i},
		time:   ts,
	}
}

func (p *point) Update() {
}

func (p *point) SetTime(t time.Time) {
}

func (p *point) Series() []byte {
	return []byte("cpu,host=server")
}

func (p *point) Fields() []Field {
	return p.fields
}

func (p *point) Time() *Timestamp {
	return p.time
}
