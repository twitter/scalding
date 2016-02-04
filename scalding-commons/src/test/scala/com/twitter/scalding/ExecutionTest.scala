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

import cascading.flow.{ FlowStep, FlowStepListener }
import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.algebird.monad.Reader
import org.apache.hadoop.conf.Configuration

// Need this to flatMap Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Promise }
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

/**
 * Run to two execution in paralel (zip) setting different config option
 * and verify that Execution.setExtraConfig are preserved in each flowStep
 *
 */
class ConfTestExecution(args: Args) extends ExecutionJob[Unit](args) {

  override def config: Map[AnyRef, AnyRef] = {
    //addFlowStepListerner to verify that Execution.setExtraConfig works
    val newconf = Config.empty.addFlowStepListener{ (m: Mode, c: Config) =>
      (m, c) match {
        case (_, _) => new TestExecutionFlowListener()
      }
    }
    super.config ++ newconf.toMap
  }

  def execution = {
    val exec1 = TypedPipe.from(TypedTsv[(String, Int)](args("input1"))).writeExecution(TypedTsv[(String, Int)](args("output1")))
    val exec2 = TypedPipe.from(TypedTsv[(String, Int)](args("input2"))).writeExecution(TypedTsv[(String, Int)](args("output2")))
    exec1.setExtraConfig(Config(Map("testconf1" -> "1")))
    exec2.setExtraConfig(Config(Map("testconf2" -> "2")))
    val zippedExec = exec1.zip(exec2)
    zippedExec.setExtraConfig(Config(Map("testconf3" -> "3")))
    zippedExec.unit
  }

  private[this] class TestExecutionFlowListener extends FlowStepListener {

    override def onStepCompleted(flowStep: FlowStep[_]): Unit = ()
    override def onStepRunning(flowStep: FlowStep[_]): Unit = ()
    override def onStepStopping(flowStep: FlowStep[_]): Unit = ()
    override def onStepStarting(flowStep: FlowStep[_]): Unit = {
      flowStep.getConfig() match {
        case conf: Configuration =>
          if (conf.get("testconf1") != null)
            ConfTestExecutionCounter.test1 = ConfTestExecutionCounter.test1 + 1;
          if (conf.get("testconf2") != null)
            ConfTestExecutionCounter.test2 = ConfTestExecutionCounter.test2 + 1;
          if (conf.get("testconf3") != null)
            ConfTestExecutionCounter.test3 = ConfTestExecutionCounter.test3 + 1;
        case _ => ()
      }
    }
    override def onStepThrowable(flowStep: FlowStep[_], throwable: Throwable): Boolean = false
  }

}

object ConfTestExecutionCounter {
  var test1: Int = 0
  var test2: Int = 0
  var test3: Int = 0
}

class ExecutionTest extends WordSpec with Matchers {
  "An Execution" should {
    "run" in {
      ExecutionTestJobs.wordCount2(TypedPipe.from(List("a b b c c c", "d d d d")))
        .waitFor(Config.default, Local(false)).get.toMap shouldBe Map("a" -> 1L, "b" -> 2L, "c" -> 3L, "d" -> 4L)
    }
    "run with zip" in {
      (ExecutionTestJobs.zipped(TypedPipe.from(0 until 100), TypedPipe.from(100 until 200))
        .waitFor(Config.default, Local(false)).get match {
          case (it1, it2) => (it1.head, it2.head)
        }) shouldBe ((0 until 100).sum, (100 until 200).sum)
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
      unorderedEq(cres, result.toList) shouldBe true
    }
    "If either fails, zip fails, else we get success" in {
      val neverHappens = Promise[Int]().future
      Execution.fromFuture { _ => neverHappens }
        .zip(Execution.failed(new Exception("oh no")))
        .waitFor(Config.default, Local(false)).isFailure shouldBe true

      Execution.failed(new Exception("oh no"))
        .zip(Execution.fromFuture { _ => neverHappens })
        .waitFor(Config.default, Local(false)).isFailure shouldBe true
      // If both are good, we succeed:
      Execution.from(1)
        .zip(Execution.from("1"))
        .waitFor(Config.default, Local(true)).get shouldBe (1, "1")
    }
  }
  "Execution K-means" should {
    "find the correct clusters for trivial cases" in {
      val dim = 20
      val k = 20
      val rng = new java.util.Random
      // if you are in cluster i, then position i == 100, else all the first k are 0.
      // Then all the tail are random, but very small enough to never bridge the gap
      def randVect(cluster: Int): Vector[Double] =
        Vector.fill(k)(0.0).updated(cluster, 100.0) ++ Vector.fill(dim - k)(rng.nextDouble / (1e6 * dim))

      // To have the seeds stay sane for kmeans k == vectorCount
      val vectorCount = k
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
          vs.foreach { case (thisId, _) => id shouldBe thisId }
      }
    }
  }
  "ExecutionApp" should {
    val parser = new ExecutionApp { def job = Execution.from(()) }
    "parse hadoop args correctly" in {
      val conf = parser.config(Array("-Dmapred.reduce.tasks=100", "--local"))._1
      conf.get("mapred.reduce.tasks") should contain ("100")
      conf.getArgs.boolean("local") shouldBe true

      val (conf1, Hdfs(_, hconf)) = parser.config(Array("--test", "-Dmapred.reduce.tasks=110", "--hdfs"))
      conf1.get("mapred.reduce.tasks") should contain ("110")
      conf1.getArgs.boolean("test") shouldBe true
      hconf.get("mapred.reduce.tasks") shouldBe "110"
    }
  }
  "An ExecutionJob" should {
    "run correctly" in {
      JobTest(new WordCountEc(_))
        .arg("input", "in")
        .arg("output", "out")
        .source(TextLine("in"), List((0, "hello world"), (1, "goodbye world")))
        .typedSink(TypedTsv[(String, Long)]("out")) { outBuf =>
          outBuf.toMap shouldBe Map("hello" -> 1L, "world" -> 2L, "goodbye" -> 1L)
        }
        .run
        .runHadoop
        .finish
    }
  }
  "Executions" should {
    "allow setting config key values on a per execution basis" in {

      val testInput: List[(String, Int)] = List(("a", 1), ("b", 2))

      // Sources
      def ourType(str: String) = TypedTsv[(String, Int)](str)

      // A method that runs a JobTest where the sources don't match
      JobTest(new ConfTestExecution(_))
        .arg("input1", "input1")
        .arg("input2", "input2")
        .arg("output1", "output1")
        .arg("output2", "output2")
        .source(ourType("input1"), testInput)
        .source(ourType("input2"), testInput)
        .sink[(String, Int)](ourType("output1")){ outBuf => { outBuf shouldBe testInput } }
        .sink[(String, Int)](ourType("output2")){ outBuf => { outBuf shouldBe testInput } }
        .runHadoop
        .finish

      ConfTestExecutionCounter.test1 shouldBe 1
      ConfTestExecutionCounter.test2 shouldBe 1
      ConfTestExecutionCounter.test3 shouldBe 2

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
      assert((first, second, third) == (1, 1, 1))
    }

    "evaluate shared portions just once, writeExecution" in {

      var timesEvaluated = 0
      val baseTp = TypedPipe.from(0 until 1000).flatMap { i =>
        timesEvaluated += 1
        List(i, i)
      }.fork

      val fde1 = baseTp.map{ _ * 3 }.writeExecution(TypedTsv("/tmp/asdf"))
      val fde2 = baseTp.map{ _ * 5 }.writeExecution(TypedTsv("/tmp/asdf2"))

      val res = fde1.zip(fde2)

      res.waitFor(Config.default, Local(true))
      assert(timesEvaluated == 1000, "Should share the common sub section of the graph when we zip two write Executions")
    }

    "evaluate shared portions just once, forceToDiskExecution" in {

      var timesEvaluated = 0
      val baseTp = TypedPipe.from(0 until 1000).flatMap { i =>
        timesEvaluated += 1
        List(i, i)
      }.fork

      val fde1 = baseTp.map{ _ * 3 }.forceToDiskExecution
      val fde2 = baseTp.map{ _ * 5 }.forceToDiskExecution

      val res = fde1.zip(fde2)

      res.waitFor(Config.default, Local(true))
      assert(timesEvaluated == 1000, "Should share the common sub section of the graph when we zip two write Executions")
    }

    "evaluate shared portions just once, forceToDiskExecution with execution cache" in {

      var timesEvaluated = 0
      val baseTp = TypedPipe.from(0 until 1000).flatMap { i =>
        timesEvaluated += 1
        List(i, i)
      }.fork

      val fde1 = baseTp.map{ _ * 3 }.forceToDiskExecution
      val fde2 = baseTp.map{ _ * 5 }.forceToDiskExecution

      val res = fde1.zip(fde2).flatMap{ _ => fde1 }.flatMap(_.toIterableExecution)

      res.waitFor(Config.default, Local(true))
      assert(timesEvaluated == 1000, "Should share the common sub section of the graph when we zip two write Executions and then flatmap")
    }
  }
}
