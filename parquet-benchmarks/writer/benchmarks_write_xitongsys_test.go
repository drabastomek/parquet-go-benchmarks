package main

import (
	"testing"
)

func BenchmarkWriterXitongsys(b *testing.B) {
	for n := 0; n < b.N; n++ {
		writeParquetXitongsys()
	}
}
