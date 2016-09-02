package lineprotocol

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type generator struct {
	i   int64
	buf []byte
}

func NewGenerator(buf []byte) *generator {
	if buf == nil {
		buf = []byte{}
	}
	return &generator{buf: buf}
}

func (g *generator) WriteTo(w io.Writer) (int64, error) {
	j := atomic.AddInt64(&g.i, 1)
	b := []byte(fmt.Sprintf("other,k=2 x=%v %v\n", j, time.Now().UnixNano()))
	n, err := w.Write(b)

	return int64(n), err

}
