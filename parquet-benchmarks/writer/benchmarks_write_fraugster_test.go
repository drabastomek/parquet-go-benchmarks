package main

import (
	"testing"
)

func BenchmarkWriterFraugster(b *testing.B) {
	for n := 0; n < b.N; n++ {
		writeParquetFraugster()
	}
}
