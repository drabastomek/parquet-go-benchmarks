package main

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type record_xitongsys struct {
	Format   string `parquet:"name=format, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DataType int32  `parquet:"name=data_type, type=INT32, encoding=PLAIN"`
	Country  string `parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

const recordNumber = 10000

func readParquetXitongsys() ([]*record_xitongsys, error) {
	fr, err := local.NewLocalFileReader("../output_xitongsys.parquet")

	if err != nil {
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, new(record_xitongsys), recordNumber)
	if err != nil {
		return nil, err
	}
	u := make([]*record_xitongsys, recordNumber)
	if err = pr.Read(&u); err != nil {
		return nil, err
	}
	pr.ReadStop()
	fr.Close()
	return u, nil
}
