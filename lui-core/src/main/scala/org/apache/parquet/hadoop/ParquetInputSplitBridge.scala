package org.apache.parquet.hadoop

object ParquetInputSplitBridge {
  def from(split: org.apache.hadoop.mapreduce.lib.input.FileSplit): ParquetInputSplit = ParquetInputSplit.from(split)
  def from(split: org.apache.hadoop.mapred.FileSplit): ParquetInputSplit = ParquetInputSplit.from(split)
}