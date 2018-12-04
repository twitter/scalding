package com.twitter.scalding.tap;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.twitter.scalding.tuple.HadoopTupleEntrySchemeIterator;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

public class ScaldingHfs extends cascading.tap.hadoop.Hfs {
  protected ScaldingHfs() {
  }

  protected ScaldingHfs(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme) {
    super(scheme);
  }

  @Deprecated
  public ScaldingHfs(Fields fields, String stringPath) {
    super(fields, stringPath);
  }

  @Deprecated
  public ScaldingHfs(Fields fields, String stringPath, boolean replace) {
    super(fields, stringPath, replace);
  }

  @Deprecated
  public ScaldingHfs(Fields fields, String stringPath, SinkMode sinkMode) {
    super(fields, stringPath, sinkMode);
  }

  public ScaldingHfs(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme, String stringPath) {
    super(scheme, stringPath);
  }

  @Deprecated
  public ScaldingHfs(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme, String stringPath, boolean replace) {
    super(scheme, stringPath, replace);
  }

  public ScaldingHfs(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme, String stringPath, SinkMode sinkMode) {
    super(scheme, stringPath, sinkMode);
  }

  @Override
  public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader input) throws IOException {
    return new HadoopTupleEntrySchemeIterator(flowProcess, this, input);
  }
}
