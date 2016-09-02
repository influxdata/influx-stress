package lineprotocol

import (
	"fmt"
	"io"
	"time"
)

type generator struct {
	n   int
	buf []byte
}

func NewGenerator(buf []byte) *generator {
	if buf == nil {
		buf = []byte{}
	}
	return &generator{buf: buf}
}

func (g *generator) WriteTo(w io.Writer) (n int64, err error) {
	b := []byte(fmt.Sprintf("other,k=2 x=%v %v\n", g.n, time.Now().UnixNano()))
	m, err := w.Write(b)
	n = int64(m)

	g.n++
	return
}
