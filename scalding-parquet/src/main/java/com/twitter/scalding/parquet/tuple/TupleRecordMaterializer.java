package com.twitter.scalding.parquet.tuple;

import cascading.tuple.Tuple;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;

public class TupleRecordMaterializer extends RecordMaterializer<Tuple> {

  private ParquetTupleConverter root;

  public TupleRecordMaterializer(GroupType parquetSchema) {
    this.root = new ParquetTupleConverter(parquetSchema);
  }

  @Override
  public Tuple getCurrentRecord() {
    return root.getCurrentTuple();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

}
