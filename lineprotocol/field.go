package lineprotocol

import (
	"io"
	"strconv"
	"sync/atomic"
)

var equalSign = byte('=')
var comma = byte(',')

type Field io.WriterTo

var (
	_ Field = &Int{}
	_ Field = &Float{}
)

type Int struct {
	Key   []byte
	Value int64
}

func (i *Int) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(i.Key)
	if err != nil {
		return int64(n), err
	}

	// Max int64 fits in 19 base-10 digits;
	// plus 1 for the leading =, plus 1 for the trailing i required for ints.
	buf := make([]byte, 0, 21)
	buf = append(buf, equalSign)
	buf = strconv.AppendInt(buf, atomic.LoadInt64(&i.Value), 10)
	buf = append(buf, 'i')

	m, err := w.Write(buf)

	return int64(n + m), err
}

type Float struct {
	Key   []byte
	Value float64
}

func (f *Float) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(f.Key)
	if err != nil {
		return int64(n), err
	}

	// Taking a total guess here at what size a float might fit in
	buf := make([]byte, 0, 32)
	buf = append(buf, equalSign)
	// There will be a data race here with *point.Update for floats
	buf = strconv.AppendFloat(buf, f.Value, 'f', -1, 64)

	m, err := w.Write(buf)

	return int64(n + m), err
}
