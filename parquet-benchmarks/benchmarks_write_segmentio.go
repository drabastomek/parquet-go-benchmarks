package main

import (
	"os"

	"github.com/segmentio/parquet-go"
)

type record_segmentio struct {
	Format   string `parquet:"name=format"`
	DataType int32  `parquet:"name=data_type"`
	Country  string `parquet:"name=country"`
}

func writeParquetSegmentio() {
	file, err := os.Create("output_segmentio.parquet")
	if err != nil {
		return
	}
	writer := parquet.NewWriter(file)

	num := 10000
	for i := 0; i < num; i++ {
		stu := record_segmentio{
			Format:   "Test",
			DataType: 1,
			Country:  "US",
		}

		if err := writer.Write(stu); err != nil {
			return
		}
	}

	// Closing the writer is necessary to flush buffers and write the file footer.
	if err := writer.Close(); err != nil {
		return
	}
}
