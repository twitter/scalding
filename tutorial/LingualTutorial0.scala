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

import com.twitter.scalding._
import com.twitter.scalding.lingual._

/*
* Very simple Lingual Job that outputs two columns from the phones data file
* Gives a simple example of how the job is setup and how to write a query
*
* To run you must use the lingual assembled jar
* ./scripts/scald.rb \
*   --lingual \
*   --local tutorial/LingualTutorial0.scala \
*   --output /tmp/lingual_test.tsv
*/
class LingualLocal(args:Args) extends Job(args) with LingualJob {
  table("bow", Tsv("tutorial/data/docBOW.tsv", fields = ('doc_id, 'word, 'count)))
  output(Tsv(args("output")))

  override def query = """select "word", "count" from "bow""""
}