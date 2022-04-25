package main

import (
	"testing"
)

func BenchmarkReaderXitongsys(b *testing.B) {
	for n := 0; n < b.N; n++ {
		readParquetXitongsys()
	}
}
