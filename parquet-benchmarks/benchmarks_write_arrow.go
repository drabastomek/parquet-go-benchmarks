package main

import (
	"os"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
)

const numOfRecords = 10000

func createArrowTable(mem memory.Allocator) arrow.Table {
	var valid []bool

	fmtValues := make([]string, numOfRecords)
	dtypeValues := make([]int32, numOfRecords)
	cntryValues := make([]string, numOfRecords)
	for i := 0; i < numOfRecords; i++ {
		fmtValues[i] = "Test"
		dtypeValues[i] = 1
		cntryValues[i] = "US"
	}

	isValid := []bool{true, true, true}
	fmt := arrow.Field{Name: "format", Type: arrow.BinaryTypes.String, Nullable: false}
	dtype := arrow.Field{Name: "data_type", Type: arrow.PrimitiveTypes.Int32, Nullable: false}
	cntry := arrow.Field{Name: "country", Type: arrow.BinaryTypes.String, Nullable: false}

	fieldList := []arrow.Field{fmt, dtype, cntry}

	arrsc := arrow.NewSchema(fieldList, nil)
	builders := make([]array.Builder, 0, len(fieldList))
	for _, f := range fieldList {
		bldr := array.NewBuilder(mem, f.Type)
		defer bldr.Release()
		builders = append(builders, bldr)
	}

	builders[0].(*array.StringBuilder).AppendValues(fmtValues, valid)
	builders[1].(*array.Int32Builder).AppendValues(dtypeValues, valid)
	builders[2].(*array.StringBuilder).AppendValues(cntryValues, valid)

	cols := make([]arrow.Column, 0, len(fieldList))

	for idx, field := range fieldList {
		arr := builders[idx].NewArray()
		defer arr.Release()

		chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
		defer chunked.Release()
		col := arrow.NewColumn(field, chunked)
		defer col.Release()
		cols = append(cols, *col)
	}

	return array.NewTable(arrsc, cols, int64(len(isValid)))
}

func writeParquetArrow() {
	file, err := os.Create("output_arrow.parquet")

	if err != nil {
		return
	}
	defer file.Close()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	tbl := createArrowTable(mem)

	pqarrow.WriteTable(
		tbl,
		file,
		tbl.NumRows(),
		nil,
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
}
