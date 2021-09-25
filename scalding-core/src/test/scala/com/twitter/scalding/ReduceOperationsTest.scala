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

import org.scalatest.{ Matchers, WordSpec }

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

class ApproximateUniqueCountJob(args: Args) extends Job(args) {
  implicit def utf8ToBytes(s: String): Array[Byte] = com.twitter.bijection.Injection.utf8(s)

  try {
    Tsv("input0", ('category, 'model, 'os)).read
      .groupBy('category) {
        _.approximateUniqueCount[String]('os -> 'os_count)
      }
      .map('os_count -> 'os_count) {
        osCount: Double => osCount.toLong
      }
      .write(Tsv("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class ReduceOperationsTest extends WordSpec with Matchers {
  import Dsl._
  val inputData = List(("a", 2L, 3.0), ("a", 3L, 3.0), ("a", 1L, 3.5), ("b", 1L, 6.0), ("b", 2L, 5.0), ("b", 3L, 4.0), ("b", 4L, 3.0), ("b", 5L, 2.0), ("b", 6L, 1.0))

  "A sortWithTake job" should {
    JobTest(new SortWithTakeJob(_))
      .source(Tsv("input0", ('key, 'item_id, 'score)), inputData)
      .sink[(String, List[(Long, Double)])](Tsv("output0")) { buf =>
        "grouped list" in {
          val whatWeWant: Map[String, String] = Map(
            "a" -> List((1L, 3.5), (3L, 3.0), (2L, 3.0)).toString,
            "b" -> List((1L, 6.0), (2L, 5.0), (3L, 4.0), (4L, 3.0), (5L, 2.0)).toString)
          val whatWeGet: Map[String, List[(Long, Double)]] = buf.toMap
          whatWeGet.get("a").getOrElse("apples") shouldBe (whatWeWant.get("a").getOrElse("oranges"))
          whatWeGet.get("b").getOrElse("apples") shouldBe (whatWeWant.get("b").getOrElse("oranges"))
        }
      }
      .runHadoop
      .finish()
  }
  "A sortedTake job" should {
    JobTest(new SortedTakeJob(_))
      .source(Tsv("input0", ('key, 'item_id, 'score)), inputData)
      .sink[(String, List[(Long, Double)])](Tsv("output0")) { buf =>
        "grouped list" in {
          val whatWeWant: Map[String, String] = Map(
            "a" -> List((1L, 3.5), (2L, 3.0), (3L, 3.0)).toString,
            "b" -> List((1L, 6.0), (2L, 5.0), (3L, 4.0), (4L, 3.0), (5L, 2.0)).toString)
          val whatWeGet: Map[String, List[(Long, Double)]] = buf.toMap
          whatWeGet.get("a").getOrElse("apples") shouldBe (whatWeWant.get("a").getOrElse("oranges"))
          whatWeGet.get("b").getOrElse("apples") shouldBe (whatWeWant.get("b").getOrElse("oranges"))
        }
      }
      .runHadoop
      .finish()
  }

  "A sortedReverseTake job" should {
    JobTest(new SortedReverseTakeJob(_))
      .source(Tsv("input0", ('key, 'item_id, 'score)), inputData)
      .sink[(String, List[(Long, Double)])](Tsv("output0")) { buf =>
        "grouped list" in {
          val whatWeWant: Map[String, String] = Map(
            "a" -> List((3L, 3.0), (2L, 3.0), (1L, 3.5)).toString,
            "b" -> List((6L, 1.0), (5L, 2.0), (4L, 3.0), (3L, 4.0), (2L, 5.0)).toString)
          val whatWeGet: Map[String, List[(Long, Double)]] = buf.toMap
          whatWeGet.get("a").getOrElse("apples") shouldBe (whatWeWant.get("a").getOrElse("oranges"))
          whatWeGet.get("b").getOrElse("apples") shouldBe (whatWeWant.get("b").getOrElse("oranges"))
        }
      }
      .runHadoop
      .finish()
  }

  "An approximateUniqueCount job" should {
    val inputData = List(
      ("laptop", "mbp 15' retina", "macosx"),
      ("mobile", "iphone5", "ios"),
      ("mobile", "droid x", "android"))

    JobTest(new ApproximateUniqueCountJob(_))
      .source(Tsv("input0", ('category, 'model, 'os)), inputData)
      .sink[(String, Long)](Tsv("output0")) { buf =>
        "grouped OS count" in {
          val whatWeWant: Map[String, Long] = Map(
            "laptop" -> 1,
            "mobile" -> 2)
          val whatWeGet: Map[String, Long] = buf.toMap
          whatWeGet should have size 2
          whatWeGet.get("laptop").getOrElse("apples") shouldBe (whatWeWant.get("laptop").getOrElse("oranges"))
          whatWeGet.get("mobile").getOrElse("apples") shouldBe (whatWeWant.get("mobile").getOrElse("oranges"))
        }
      }
      .runHadoop
      .finish()
  }
}
