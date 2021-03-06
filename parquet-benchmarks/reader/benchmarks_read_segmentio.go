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

func readParquetSegmentio() {
	file, err := os.Open("../output_segmentio.parquet")
	if err != nil {
		return
	}
	reader := parquet.NewReader(file)

	for {
		stu := new(record_segmentio)

		if err := reader.Read(stu); err != nil {
			return
		}
	}

	// // Closing the reader is necessary to flush buffers and write the file footer.
	// if err := file.Close(); err != nil {
	// 	return
	// }
}
