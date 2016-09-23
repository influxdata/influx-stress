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
	precision Precision
	ptr       unsafe.Pointer
}

func NewTimestamp(p Precision) *Timestamp {
	return &Timestamp{
		precision: p,
	}
}

func (t *Timestamp) TimePtr() *unsafe.Pointer {
	return &t.ptr
}

func (t *Timestamp) SetTime(ts *time.Time) {
	tsPtr := unsafe.Pointer(ts)
	atomic.StorePointer(&t.ptr, tsPtr)
}

func (t *Timestamp) WriteTo(w io.Writer) (int64, error) {
	tsPtr := atomic.LoadPointer(&t.ptr)

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
