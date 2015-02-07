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

package com.twitter.scalding_internal.db.jdbc

import java.io.IOException

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.tap.SinkMode
import cascading.tap.hadoop.Hfs
import cascading.tuple.TupleEntryIterator

import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

class JdbcSinkHfsTap(scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _],
  stringPath: String, @transient completionHandler: JdbcSinkCompletionHandler)
  extends Hfs(scheme, stringPath, SinkMode.REPLACE) {

  override def commitResource(conf: JobConf) = {
    val superResult = super.commitResource(conf)
    if (superResult)
      completionHandler.commitResource(conf, stringPath)
    else
      superResult
  }

  override def openForRead(flowProcess: FlowProcess[JobConf],
    input: RecordReader[_, _]): TupleEntryIterator =
    throw new IOException("Reading not supported")
}

