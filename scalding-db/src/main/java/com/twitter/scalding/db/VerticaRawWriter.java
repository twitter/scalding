/*
Copyright 2015 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.db;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 * A writer for Vertica's native file format to HDFS. The output is used to
 * load data into Vertica using the COPY command.
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


