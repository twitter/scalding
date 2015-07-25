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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;

/**
 * Cascading tap for exporting data to Vertica using the COPY command.
 *
 * Output data is first staged in HDFS using Vertica's native file format
 * and the COPY command is executed as part of the completion handler.
 */
public class VerticaSinkTap extends Hfs {
  private transient JdbcSinkCompletionHandler completionHandler;

  public VerticaSinkTap(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme,
                        JobConf conf, SinkMode sinkMode,
                        JdbcSinkCompletionHandler completionHandler) {
    this(scheme, conf, sinkMode, completionHandler, initTemporaryPath(conf, true));
  }

  public VerticaSinkTap(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme,
                        JobConf conf, SinkMode sinkMode,
                        JdbcSinkCompletionHandler completionHandler,
                        String targetPath) {
    super(scheme, targetPath, sinkMode);
    this.completionHandler = completionHandler;
  }

  private static String initTemporaryPath(JobConf conf, boolean unique) {
    return new Path(getTempPath(conf),
      "Vrt" + Util.createUniqueID().substring(0, 5)).toString();
  }

  @Override
  public boolean commitResource(JobConf conf) throws IOException {
    boolean superResult = super.commitResource(conf);
    if (superResult) {
      return completionHandler.commitResource(conf, stringPath);
    } else {
      return superResult;
    }
  }

  public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
                                        RecordReader input) throws IOException {
    throw new IOException("Reading not supported");
  }
}
