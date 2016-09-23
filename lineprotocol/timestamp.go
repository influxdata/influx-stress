package lineprotocol

import (
	"io"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

type Precision int

const (
	Nanosecond = iota
	Second
)

type Timestamp struct {
	Time      time.Time
	precision Precision
}

func NewTimestamp(p Precision) *Timestamp {
	return &Timestamp{
		precision: p,
	}
}

func (t *Timestamp) WriteTo(w io.Writer) (int64, error) {
	unsafeTime := unsafe.Pointer(&t.Time)
	tsPtr := atomic.LoadPointer(&unsafeTime)

	tsTime := *(*time.Time)(tsPtr)
	ts := tsTime.UnixNano()

	if t.precision == Second {
		ts = tsTime.Unix()
	}

	// Max int64 fits in 19 base-10 digits;
	buf := make([]byte, 0, 19)
	buf = strconv.AppendInt(buf, ts, 10)

	n, err := w.Write(buf)
	return int64(n), err
}
