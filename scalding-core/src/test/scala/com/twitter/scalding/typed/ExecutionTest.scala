/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.scalding.typed

import org.specs._

import com.twitter.scalding._
import com.twitter.algebird.monad.Reader

// Need this to flatMap Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.util.Try

import com.twitter.scalding.examples.KMeans

// this is the scalding ExecutionContext
import ExecutionContext._

object ExecutionTestJobs {
  def wordCount(in: String, out: String) =
    TypedPipe.from(TextLine(in))
      .flatMap(_.split("\\s+"))
      .map((_, 1L))
      .sumByKey
      .writeExecution(TypedTsv(out))

  def wordCount2(in: TypedPipe[String]) =
    in
      .flatMap(_.split("\\s+"))
      .map((_, 1L))
      .sumByKey
      .toIterableExecution

  def zipped(in1: TypedPipe[Int], in2: TypedPipe[Int]) =
    in1.groupAll.sum.values.toIterableExecution
      .zip(in2.groupAll.sum.values.toIterableExecution)

  def mergeFanout(in: List[Int]): Execution[Iterable[(Int, Int)]] = {
    // Force a reduce, so no fancy optimizations kick in
    val source = TypedPipe.from(in).groupBy(_ % 3).head

    (source.mapValues(_ * 2) ++ (source.mapValues(_ * 3))).toIterableExecution
  }
}

class WordCountEc(args: Args) extends ExecutionJob[Unit](args) {
  def execution = ExecutionTestJobs.wordCount(args("input"), args("output"))
  // In tests, classloader issues with sbt mean we should not
  // really use threads, so we run immediately
  override def concurrentExecutionContext = new scala.concurrent.ExecutionContext {
    def execute(r: Runnable) = r.run
    def reportFailure(t: Throwable) = ()
  }
}

class ExecutionTest extends Specification {
  "An Executor" should {
    "work synchonously" in {
      val (r, stats) = Execution.waitFor(Config.default, Local(false)) { implicit ec: ExecutionContext =>
        val sink = new MemorySink[(Int, Int)]
        TypedPipe.from(0 to 100)
          .map { k => (k % 3, k) }
          .sumByKey
          .write(sink)

        { () => sink.readResults }
      }
      stats.isSuccess must beTrue
      r().toMap must be_==((0 to 100).groupBy(_ % 3).mapValues(_.sum).toMap)
    }
    "work asynchonously" in {
      val (r, fstats) = Execution.run(Config.default, Local(false)) { implicit ec: ExecutionContext =>
        val sink = new MemorySink[(Int, Int)]
        TypedPipe.from(0 to 100)
          .map { k => (k % 3, k) }
          .sumByKey
          .write(sink)

        { () => sink.readResults }
      }
      Try(Await.result(fstats, Duration.Inf)).isSuccess must beTrue
      r().toMap must be_==((0 to 100).groupBy(_ % 3).mapValues(_.sum).toMap)
    }
  }
  "An Execution" should {
    "run" in {
      ExecutionTestJobs.wordCount2(TypedPipe.from(List("a b b c c c", "d d d d")))
        .waitFor(Config.default, Local(false)).get.toMap must be_==(
          Map("a" -> 1L, "b" -> 2L, "c" -> 3L, "d" -> 4L))
    }
    "run with zip" in {
      (ExecutionTestJobs.zipped(TypedPipe.from(0 until 100), TypedPipe.from(100 until 200))
        .waitFor(Config.default, Local(false)).get match {
          case (it1, it2) => (it1.head, it2.head)
        }) must be_==((0 until 100).sum, (100 until 200).sum)
    }
    "merge fanouts without error" in {
      def unorderedEq[T](l: Iterable[T], r: Iterable[T]): Boolean =
        (l.size == r.size) && (l.toSet == r.toSet)

      def correct(l: List[Int]): List[(Int, Int)] = {
        val in = l.groupBy(_ % 3).mapValues(_.head)
        in.mapValues(_ * 2).toList ++ in.mapValues(_ * 3)
      }
      val input = (0 to 100).toList
      val result = ExecutionTestJobs.mergeFanout(input).waitFor(Config.default, Local(false)).get
      val cres = correct(input)
      unorderedEq(cres, result.toList) must beTrue
    }
  }
  "Execution K-means" should {
    "find the correct clusters for trivial cases" in {
      val dim = 20
      val k = 5
      val rng = new java.util.Random
      // if you are in cluster i, then position i == 1, else all the first k are 0.
      // Then all the tail are random, but small enough to never bridge the gap
      def randVect(cluster: Int): Vector[Double] =
        Vector.fill(k)(0.0).updated(cluster, 1.0) ++ Vector.fill(dim - k)(rng.nextDouble / dim)

      val vectorCount = 1000
      val vectors = TypedPipe.from((0 until vectorCount).map { i => randVect(i % k) })

      val labels = KMeans(k, vectors).flatMap {
        case (_, _, labeledPipe) =>
          labeledPipe.toIterableExecution
      }
        .waitFor(Config.default, Local(false)).get.toList

      def clusterOf(v: Vector[Double]): Int = v.indexWhere(_ > 0.0)

      val byCluster = labels.groupBy { case (id, v) => clusterOf(v) }

      // The rule is this: if two vectors share the same prefix,
      // the should be in the same cluster
      byCluster.foreach {
        case (clusterId, vs) =>
          val id = vs.head._1
          vs.forall { case (thisId, _) => id must be_==(thisId) }
      }
    }
  }
  "ExecutionApp" should {
    val parser = new ExecutionApp { def job = Execution.from(()) }
    "parse hadoop args correctly" in {
      val conf = parser.config(Array("-Dmapred.reduce.tasks=100", "--local"))._1
      conf.get("mapred.reduce.tasks") must be_==(Some("100"))
      conf.getArgs.boolean("local") must beTrue

      val (conf1, Hdfs(_, hconf)) = parser.config(Array("--test", "-Dmapred.reduce.tasks=110", "--hdfs"))
      conf1.get("mapred.reduce.tasks") must be_==(Some("110"))
      conf1.getArgs.boolean("test") must beTrue
      hconf.get("mapred.reduce.tasks") must be_==("110")
    }
  }
  "An ExecutionJob" should {
    "run correctly" in {
      JobTest(new com.twitter.scalding.typed.WordCountEc(_))
        .arg("input", "in")
        .arg("output", "out")
        .source(TextLine("in"), List((0, "hello world"), (1, "goodbye world")))
        .sink[(String, Long)](TypedTsv[(String, Long)]("out")) { outBuf =>
          val results = outBuf.toMap
          results must be_==(Map("hello" -> 1L, "world" -> 2L, "goodbye" -> 1L))
        }
        .run
        .runHadoop
        .finish
    }
  }
  "Executions" should {
    "evaluate once per run" in {
      var first = 0
      var second = 0
      var third = 0
      val e1 = Execution.from({ first += 1; 42 })
      val e2 = e1.flatMap { x =>
        second += 1
        Execution.from(2 * x)
      }
      val e3 = e1.map { x => third += 1; x * 3 }
      /**
       * Notice both e3 and e2 need to evaluate e1.
       */
      val res = e3.zip(e2)
      res.waitFor(Config.default, Local(true))
      (first, second, third) must be_==((1, 1, 1))
    }
  }
}
