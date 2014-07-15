/*
Copyright 2012 Twitter, Inc.

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

package com.twitter.scalding.examples

import cascading.tuple.Fields
import cascading.flow.{Flow, FlowDef}
import com.twitter.scalding._
import com.twitter.scalding.lingual._

/**
  Another simple Local job that uses a Lingual Table to do some preprocessing on the data source.

  Currently Scalding-Lingual does not support nested structures such as thrift in queries so
  scalding must be used to flatten these types

  This LocalTable is none necessary as it just removes the first column but shows how Tables can be created.

  Also shows a slightly more complicated query where a group and count is used.
 */

case class LocalTable()(implicit flowDef:FlowDef) extends FlatMappedLingualTable[(Int, String, Int), (String, Int)] {
  override def fieldNames = new Fields("cnv_word", "cnv_count")
  override def function = {case(index, word, count) => Some((word, count))}
  override def tmpDirectory = "/tmp"
  override def name = "bow"
  override def source = TypedTsv[(Int, String, Int)]("tutorial/data/docBOW.tsv", f=new Fields("doc_id", "word", "count"))
}

class LingualTableLocal(args:Args) extends Job(args) with LingualJob {
  table(LocalTable())
  output(Tsv(args("output")))

  override def query = """select "cnv_count", "cnv_word" from "bow" """
}