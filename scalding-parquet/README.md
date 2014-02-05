# Parquet support for Scalding

This module contains Parquet support for Scalding. It's based on the parquet-cascading module of [parquet-mr](https://github.com/Parquet/parquet-mr) project.

## Usage

Read specific fields from parquet file:
```
import com.twitter.scalding.parquet.ParquetSource

ParquetSource(('platform, 'storage_key), args("input"))
.read
.groupBy('platform) {_.size('count)}
```

Read ALL fields from parquet file:
```
import com.twitter.scalding.parquet.ParquetSource
import cascading.tuple.Fields

ParquetSource(Fields.ALL, args("input"))
.read
.groupBy('platform) {_.size('count)}
```

## How to add to your project

TBD

## TODO

1. write tests for ParquetSource
2. support for nested fields (not implemented in [ParquetTupleScheme](https://github.com/Parquet/parquet-mr/blob/master/parquet-cascading/src/main/java/parquet/cascading/ParquetTupleScheme.java) yet).
