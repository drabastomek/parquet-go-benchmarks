package main

import (
	"context"

	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
)

func readParquetArrow() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	file, err := file.OpenParquetFile("../output_arrow.parquet", false)

	if err != nil {
		return
	}
	defer file.Close()

	arrowRdr, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return
	}

	tbl, err := arrowRdr.ReadTable(context.Background())
	if err != nil {
		return
	}
	defer tbl.Release()

}
