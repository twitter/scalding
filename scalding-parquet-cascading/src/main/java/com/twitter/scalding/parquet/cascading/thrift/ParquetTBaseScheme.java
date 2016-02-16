package com.twitter.scalding.parquet.cascading.thrift;

import com.twitter.scalding.parquet.cascading.ParquetValueScheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.thrift.TBase;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;
import org.apache.parquet.hadoop.thrift.TBaseWriteSupport;
import org.apache.parquet.thrift.TBaseRecordConverter;

public class ParquetTBaseScheme<T extends TBase<?,?>> extends ParquetValueScheme<T> {

  // In the case of reads, we can read the thrift class from the file metadata
  public ParquetTBaseScheme() {
    this(new Config<T>());
  }

  public ParquetTBaseScheme(Class<T> thriftClass) {
    this(new Config<T>().withRecordClass(thriftClass));
  }

  public ParquetTBaseScheme(FilterPredicate filterPredicate) {
    this(new Config<T>().withFilterPredicate(filterPredicate));
  }

  public ParquetTBaseScheme(FilterPredicate filterPredicate, Class<T> thriftClass) {
    this(new Config<T>().withRecordClass(thriftClass).withFilterPredicate(filterPredicate));
  }

  public ParquetTBaseScheme(ParquetValueScheme.Config<T> config) {
    super(config);
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    super.sourceConfInit(fp, tap, jobConf);
    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, ThriftReadSupport.class);
    ThriftReadSupport.setRecordConverterClass(jobConf, TBaseRecordConverter.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<? extends JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

    if (this.config.getKlass() == null) {
      throw new IllegalArgumentException("To use ParquetTBaseScheme as a sink, you must specify a thrift class in the constructor");
    }

    DeprecatedParquetOutputFormat.setAsOutputFormat(jobConf);
    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, TBaseWriteSupport.class);
    TBaseWriteSupport.<T>setThriftClass(jobConf, this.config.getKlass());
  }
}
