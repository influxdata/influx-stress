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
