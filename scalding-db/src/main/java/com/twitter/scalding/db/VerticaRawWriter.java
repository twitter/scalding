package com.twitter.scalding.db;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 * A writer for LZO-encoded blocks of protobuf or Thrift objects, generally read by
 * a ProtobufBlockWriter or similar.
 */
public class VerticaRawWriter
    implements RecordWriter<NullWritable, VerticaRowWrapper> {
  private final OutputStream out_;

  public VerticaRawWriter(OutputStream out) {
    out_ = out;
  }

  public void write(NullWritable nullWritable, VerticaRowWrapper valueToWrite)
      throws IOException  {
    valueToWrite.sink(out_);
  }

  public void close(Reporter reporter)
      throws IOException {
    out_.close();
  }
}


