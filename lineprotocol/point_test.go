package lineprotocol

import (
	"testing"
	"time"
)

type mockPoint struct{}

func NewMockPoint() Point {
	return &mockPoint{}
}

func (p *mockPoint) Series() []byte {
	return []byte("cpu,host=server")
}

func (p *mockPoint) Fields() []Field {
	i := &Int{
		Key:   []byte("value"),
		Value: int64(100),
	}
	return []Field{i}
}

func (p *mockPoint) Time() *Timestamp {
	ts := NewTimestamp(Nanosecond)
	ts.Time = time.Now()
	return ts
}

// Tests Start Here //
func TestWritePoint_MockPoint(t *testing.T) {}

func TestWritePoint_point(t *testing.T) {}

func TestNewPoint(t *testing.T) {}

func TestPoint_Series(t *testing.T) {}
func TestPoint_Fields(t *testing.T) {}
func TestPoint_Time(t *testing.T)   {}

func BenchmarkWritePoint_MockPoint(b *testing.B) {}
func BenchmarkWritePoint_point(b *testing.B)     {}
