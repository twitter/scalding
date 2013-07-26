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
package com.twitter.scalding

import org.specs._
import com.twitter.scalding._

class SortWithTakeJob(args: Args) extends Job(args) {
  try {
    Tsv("input0", ('key, 'item_id, 'score)).read
      .groupBy('key) {
        _.sortWithTake[(Long, Double)]((('item_id, 'score), 'top_items), 5) {
          (item_0: (Long, Double), item_1: (Long, Double)) => if (item_0._2 == item_1._2) { item_0._1 > item_1._1 } else { item_0._2 > item_1._2 }
        }
      }
      .map('top_items -> 'top_items) {
        //used to test that types are correct
        topItems: List[(Long, Double)] => topItems
      }
      .project('key, 'top_items)
      .write(Tsv("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class SortedReverseTakeJob(args: Args) extends Job(args) {
  try {
    Tsv("input0", ('key, 'item_id, 'score)).read
      .groupBy('key) {
        _.sortedReverseTake[(Long, Double)]((('item_id, 'score), 'top_items), 5)
      }
      .map('top_items -> 'top_items) {
        //used to test that types are correct
        topItems: List[(Long, Double)] => topItems
      }
      .project('key, 'top_items)
      .write(Tsv("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class SortedTakeJob(args: Args) extends Job(args) {
  try {
    Tsv("input0", ('key, 'item_id, 'score)).read
      .groupBy('key) {
        _.sortedTake[(Long, Double)]((('item_id, 'score), 'top_items), 5)
      }
      .map('top_items -> 'top_items) {
        //used to test that types are correct
        topItems: List[(Long, Double)] => topItems
      }
      .project('key, 'top_items)
      .write(Tsv("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class ReduceOperationsTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  val inputData = List(("a", 2L, 3.0), ("a", 3L, 3.0), ("a", 1L, 3.5), ("b", 1L, 6.0), ("b", 2L, 5.0), ("b", 3L, 4.0), ("b", 4L, 3.0), ("b", 5L, 2.0), ("b", 6L, 1.0))

  "A sortWithTake job" should {
    JobTest("com.twitter.scalding.SortWithTakeJob")
      .source(Tsv("input0", ('key, 'item_id, 'score)), inputData)
      .sink[(String, List[(Long, Double)])](Tsv("output0")) { buf =>
        "grouped list" in {
          val whatWeWant: Map[String, String] = Map(
              "a" -> List((1L, 3.5), (3L, 3.0), (2L, 3.0)).toString,
              "b" -> List((1L, 6.0), (2L, 5.0), (3L, 4.0), (4L, 3.0), (5L, 2.0)).toString)
          val whatWeGet: Map[String, List[(Long, Double)]] = buf.toMap
          whatWeGet.get("a").getOrElse("apples") must be_==(whatWeWant.get("a").getOrElse("oranges"))
          whatWeGet.get("b").getOrElse("apples") must be_==(whatWeWant.get("b").getOrElse("oranges"))
        }
      }
      .runHadoop
      .finish
  }
  "A sortedTake job" should {
    JobTest("com.twitter.scalding.SortedTakeJob")
      .source(Tsv("input0", ('key, 'item_id, 'score)), inputData)
      .sink[(String, List[(Long, Double)])](Tsv("output0")) { buf =>
        "grouped list" in {
          val whatWeWant: Map[String, String] = Map(
              "a" -> List((1L, 3.5), (2L, 3.0), (3L, 3.0)).toString,
              "b" -> List((1L, 6.0), (2L, 5.0), (3L, 4.0), (4L, 3.0), (5L, 2.0)).toString)
          val whatWeGet: Map[String, List[(Long, Double)]] = buf.toMap
          whatWeGet.get("a").getOrElse("apples") must be_==(whatWeWant.get("a").getOrElse("oranges"))
          whatWeGet.get("b").getOrElse("apples") must be_==(whatWeWant.get("b").getOrElse("oranges"))
        }
      }
      .runHadoop
      .finish
  }

  "A sortedReverseTake job" should {
    JobTest("com.twitter.scalding.SortedReverseTakeJob")
      .source(Tsv("input0", ('key, 'item_id, 'score)), inputData)
      .sink[(String, List[(Long, Double)])](Tsv("output0")) { buf =>
        "grouped list" in {
          val whatWeWant: Map[String, String] = Map(
              "a" -> List((3L, 3.0), (2L, 3.0), (1L, 3.5)).toString,
              "b" -> List((6L, 1.0), (5L, 2.0), (4L, 3.0), (3L, 4.0), (2L, 5.0)).toString)
          val whatWeGet: Map[String, List[(Long, Double)]] = buf.toMap
          whatWeGet.get("a").getOrElse("apples") must be_==(whatWeWant.get("a").getOrElse("oranges"))
          whatWeGet.get("b").getOrElse("apples") must be_==(whatWeWant.get("b").getOrElse("oranges"))
        }
      }
      .runHadoop
      .finish
  }
}
