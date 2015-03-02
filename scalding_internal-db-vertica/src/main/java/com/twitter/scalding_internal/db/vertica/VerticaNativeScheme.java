package com.twitter.scalding_internal.db.vertica;


import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import java.io.IOException;
import cascading.tuple.TupleEntry;

@SuppressWarnings("unchecked")
public class VerticaNativeScheme<M> extends Scheme<JobConf, RecordReader, OutputCollector, Void, VerticaRowWrapper>
{
  private static final long serialVersionUID = 32756348231137L;
  private final VerticaRowWrapperFactory<M> _wrapperFactory;
  private final VerticaHeader<M> _header;

  public VerticaNativeScheme(VerticaRowWrapperFactory<M> wrapperFactory, VerticaHeader<M> header) {
    super();
    _wrapperFactory = wrapperFactory;
    _header = header;
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> hfp, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    VerticaUtils.setHeaderContents(conf, _header);
    conf.setOutputFormat(VerticaNativeOutputFormat.class);
    conf.set("mapreduce.output.fileoutputformat.compress", "false");
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<VerticaRowWrapper, OutputCollector> sinkCall)
    throws IOException {
    OutputCollector collector = sinkCall.getOutput();
    TupleEntry entry = sinkCall.getOutgoingEntry();


    M obj = (M)entry.getTuple().getObject(0);
    VerticaRowWrapper wrapped = _wrapperFactory.wrap(obj);

    collector.collect(null, wrapped);
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
    SourceCall<Void, RecordReader> sourceCall) throws IOException {
    throw new IOException("Not able to use as source");
  }

  @Override public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf conf) {
  }

}
