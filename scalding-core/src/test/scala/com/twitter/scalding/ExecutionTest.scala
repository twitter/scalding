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
package com.twitter.scalding

import com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MacroEqualityOrderedSerialization
import com.twitter.scalding.serialization.OrderedSerialization
import java.nio.file.Files
import java.io.File
import java.util
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise, ExecutionContext => ConcurrentExecutionContext}
import scala.util.{Failure, Success, Try}
import cascading.flow.{Flow, FlowDef, FlowListener}
import com.twitter.scalding.typed.cascading_backend.AsyncFlowDefRunner.TempFileCleanup
import org.apache.hadoop.conf.Configuration

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

  def writeExecutionWithTempFile(tempFile: String, testData: List[String]): Execution[List[String]] = {
    val forced = TypedPipe.from(testData).map(s => s)
      .forceToDiskExecution

    Execution.withConfig(forced) { conf => conf + ("hadoop.tmp.dir" -> tempFile) }
      .flatMap(_.toIterableExecution)
      .map(_.toList)
  }
}

abstract class TestExecutionJob[+T](args: Args) extends ExecutionJob[T](args) {
  // In tests, classloader issues with sbt mean we should not
  // really use threads, so we run immediately
  override def concurrentExecutionContext = new scala.concurrent.ExecutionContext {
    def execute(r: Runnable) = r.run
    def reportFailure(t: Throwable) = ()
  }
}

class WordCountEc(args: Args) extends TestExecutionJob[Unit](args) {
  def execution = ExecutionTestJobs.wordCount(args("input"), args("output"))
}

class ExecutionWithTempFiles(args: Args, tempFile: String, testData: List[String]) extends TestExecutionJob[List[String]](args) {
  override def execution = ExecutionTestJobs.writeExecutionWithTempFile(tempFile, testData)
}

class ZippedExecutionWithTempFiles(args: Args, tempFileOne: String, tempFileTwo: String, testDataOne: List[String], testDataTwo: List[String]) extends TestExecutionJob[(List[String], List[String])](args) {
  override def execution = {
    val executionOne = ExecutionTestJobs.writeExecutionWithTempFile(tempFileOne, testDataOne)
    val executionTwo = ExecutionTestJobs.writeExecutionWithTempFile(tempFileTwo, testDataTwo)
    executionOne.zip(executionTwo)
  }
}

case class MyCustomType(s: String)

class NormalJobToExecutionTestJob(args: Args) extends Job(args) {
  TypedPipe.from(0 to 100)
    .groupBy(_ % 3)
    .sum
    .write(source.NullSink)
}

class FlowListenerWithException extends FlowListener {
  override def onStarting(flow: Flow[_]): Unit = {
    throw new RuntimeException("something goes wrong")
  }

  override def onCompleted(flow: Flow[_]): Unit = {}

  override def onStopping(flow: Flow[_]): Unit = {}

  override def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = false
}

class ExecutionTest extends WordSpec with Matchers {
  implicit class ExecutionTestHelper[T](ex: Execution[T]) {
    def shouldSucceed(): T = {
      val r = ex.waitFor(Config.default, Local(true))
      r match {
        case Success(s) => s
        case Failure(e) => fail(s"Failed running execution, exception:\n$e")
      }
    }
    def shouldSucceedHadoop(): T = {
      val mode = Hdfs(true, new Configuration)
      val r = ex.waitFor(Config.defaultFrom(mode), mode)
      r match {
        case Success(s) => s
        case Failure(e) => fail(s"Failed running execution, exception:\n$e")
      }
    }
    def shouldFail(): Unit = {
      val r = ex.waitFor(Config.default, Local(true))
      assert(r.isFailure)
    }
    def shouldFailWith(message: String): Unit = {
      val r = ex.waitFor(Config.default, Local(true))
      assert(r.isFailure)
      r.failed.get.getMessage shouldBe message
    }
  }

  def getShutdownHooks: Seq[Thread] = {
    // The list of attached shutdown hooks are not accessible normally, so we must use reflection to get them
    val clazz = Class.forName("java.lang.ApplicationShutdownHooks")
    val hooksField = clazz.getDeclaredField("hooks")
    hooksField.setAccessible(true)
    hooksField.get(null).asInstanceOf[util.IdentityHashMap[Thread, Thread]].asScala.keys.toSeq
  }

  def isTempFileCleanupHook(hook: Thread): Boolean =
    classOf[TempFileCleanup].isAssignableFrom(hook.getClass)

  "An Execution" should {
    "run" in {
      ExecutionTestJobs.wordCount2(TypedPipe.from(List("a b b c c c", "d d d d")))
        .waitFor(Config.default, Local(false)).get.toMap shouldBe Map("a" -> 1L, "b" -> 2L, "c" -> 3L, "d" -> 4L)
    }
    "run with zip" in {
      (ExecutionTestJobs.zipped(TypedPipe.from(0 until 100), TypedPipe.from(100 until 200))
        .shouldSucceed() match {
          case (it1, it2) => (it1.head, it2.head)
        }) shouldBe ((0 until 100).sum, (100 until 200).sum)
    }
    "run with exception in flow listener" in {
      val exec = ExecutionTestJobs.wordCount2(TypedPipe.from(List("a", "b")))

      Execution.withConfig(exec) { config =>
        config.addFlowListener((_, _) => new FlowListenerWithException())
      }.shouldFailWith("Flow was stopped")
    }
    "lift to try" in {
      val res = ExecutionTestJobs
        .wordCount2(TypedPipe.from(List("a", "b")))
        .liftToTry
        .shouldSucceed()

      assert(res.isSuccess)
    }
    "lift to try on exception" in {
      val res: Try[Nothing] = ExecutionTestJobs
        .wordCount2(TypedPipe.from(List("a", "b")))
        .map(_ => throw new RuntimeException("Something went wrong"))
        .liftToTry
        .shouldSucceed()

      assert(res.isFailure)
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
        .shouldFail()

      Execution.failed(new Exception("oh no"))
        .zip(Execution.fromFuture { _ => neverHappens })
        .shouldFail()
      // If both are good, we succeed:
      Execution.from(1)
        .zip(Execution.from("1"))
        .shouldSucceed() shouldBe (1, "1")
    }

    "Config transformer will isolate Configs" in {
      def doesNotHaveVariable(message: String) = Execution.getConfig.flatMap { cfg =>
        if (cfg.get("test.cfg.variable").isDefined)
          Execution.failed(new Exception(s"${message}\n: var: ${cfg.get("test.cfg.variable")}"))
        else
          Execution.from(())
      }

      val hasVariable = Execution.getConfig.flatMap { cfg =>
        if (cfg.get("test.cfg.variable").isEmpty)
          Execution.failed(new Exception("Should see variable inside of transform"))
        else
          Execution.from(())
      }

      def addOption(cfg: Config) = cfg.+ ("test.cfg.variable", "dummyValue")

      doesNotHaveVariable("Should not see variable before we've started transforming")
        .flatMap{ _ => Execution.withConfig(hasVariable)(addOption) }
        .flatMap(_ => doesNotHaveVariable("Should not see variable in flatMap's after the isolation"))
        .map(_ => true)
        .shouldSucceed() shouldBe true
    }

    "Config transformer will interact correctly with the cache" in {
      var incrementIfDefined = 0
      var totalEvals = 0

      val incrementor = Execution.getConfig.flatMap { cfg =>
        totalEvals += 1
        if (cfg.get("test.cfg.variable").isDefined)
          incrementIfDefined += 1
        Execution.from(())
      }

      def addOption(cfg: Config) = cfg.+ ("test.cfg.variable", "dummyValue")

      // Here we run without the option, with the option, and finally without again.
      incrementor
        .flatMap{ _ => Execution.withConfig(incrementor)(addOption) }
        .flatMap(_ => incrementor)
        .map(_ => true)
        .shouldSucceed() shouldBe true

      assert(incrementIfDefined === 1)
      // We should evaluate once for the default config, and once for the modified config.
      assert(totalEvals === 2)
    }

    "Config transformer will interact correctly with the cache when writing" in {
      import java.io._
      val srcF = File.createTempFile("tmpoutputLocation", ".tmp").getAbsolutePath
      val sinkF = File.createTempFile("tmpoutputLocation2", ".tmp").getAbsolutePath

      def writeNums(nums: List[Int]): Unit = {
        val pw = new PrintWriter(new File(srcF))
        pw.write(nums.mkString("\n"))
        pw.close
      }

      writeNums(List(1, 2, 3))

      val sink = TypedTsv[Int](sinkF)
      val src = TypedTsv[Int](srcF)
      val operationTP = (TypedPipe.from(src) ++ TypedPipe.from((1 until 100).toList)).writeExecution(sink).getCounters.map(_._2.toMap)

      def addOption(cfg: Config) = cfg.+ ("test.cfg.variable", "dummyValue")

      // Here we run without the option, with the option, and finally without again.
      val (oldCounters, newCounters) = operationTP
        .flatMap{ oc =>
          writeNums(List(1, 2, 3, 4, 5, 6, 7))
          Execution.withConfig(operationTP)(addOption).map { nc => (oc, nc) }
        }
        .shouldSucceed()

      assert(oldCounters != newCounters, "With new configs given the source changed we shouldn't cache so the counters should be different")

    }

    "correctly add cached file into config" in {
      val execution = Execution.withCachedFile("/path/to/your/file.txt") { cachedFile =>
        Execution.getConfig.map { config =>
          config.getDistributedCachedFiles should contain only cachedFile
        }
      }

      execution.waitFor(Config.default, Hdfs(strict = true, new Configuration(false))) match {
        case Success(s) => s
        case Failure(e) => fail(s"Failed running execution, exception:\n$e")
      }
    }

    "correctly add cached files into config" in {
      val execution =
        Execution.withCachedFile("/path/to/your/one.txt") { one =>
          Execution.withCachedFile("/path/to/your/second.txt") { second =>
            Execution.getConfig.map { config =>
              config.getDistributedCachedFiles should contain only (one, second)
            }
          }
        }

      execution.waitFor(Config.default, Hdfs(strict = true, new Configuration(false))) match {
        case Success(s) => s
        case Failure(e) => fail(s"Failed running execution, exception:\n$e")
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
        .finish()
    }
  }
  "Executions" should {
    "work correctly with flowDef from user" in {
      class PipeBuilderJob(args: Args) extends TestExecutionJob[Unit](args) {
        override def execution: Execution[Unit] =
          Execution.getMode.flatMap { mode =>
            val flowDef: FlowDef = new FlowDef

            pipeBuilder(flowDef, mode)

            Execution.fromFn((_, _) => flowDef)
          }

        def pipeBuilder(implicit flowDef: FlowDef, mode: Mode): TypedPipe[Int] = {
          TypedPipe.from(TextLine(args("input")))
            .map(_.toInt)
            .map(_ * 2)
            .write(TypedTsv[Int]("out"))
        }
      }

      val input = List((0, "1"), (1, "2"), (2, "3"), (3, "4"), (4, "5"))
      val expected = input.map(_._2).map(_.toInt).map(_ * 2)

      JobTest(new PipeBuilderJob(_))
        .arg("input", "in")
        .source(TextLine("in"), input)
        .typedSink(TypedTsv[Int]("out")) { outBuf =>
          outBuf.toList shouldBe expected
        }
        .run
        .runHadoop
        .finish()
    }

    "shutdown hook should clean up temporary files" in {
      val tempFileOne = Files.createTempDirectory("scalding-execution-test")
      val tempFileTwo = Files.createTempDirectory("scalding-execution-test")
      val mode = Test(Map())

      Files.exists(tempFileOne) should be(true)
      Files.exists(tempFileTwo) should be(true)

      val cleanupThread = TempFileCleanup(List(tempFileOne.toFile.getAbsolutePath, tempFileTwo.toFile.getAbsolutePath), mode)
      cleanupThread.run()

      Files.exists(tempFileOne) should be(false)
      Files.exists(tempFileTwo) should be(false)
    }

    "clean up temporary files on exit" in {
      val tempFile = Files.createTempDirectory("scalding-execution-test").toFile.getAbsolutePath
      val testData = List("a", "b", "c")
      getShutdownHooks.foreach { hook: Thread =>
        isTempFileCleanupHook(hook) should be(false)
      }

      ExecutionTestJobs.writeExecutionWithTempFile(tempFile, testData)
        .shouldSucceedHadoop()

      // This is hacky, but there's a small chance that the new cleanup hook isn't registered by the time we get here
      // A small sleep like this appears to be sufficient to ensure we can see it
      Thread.sleep(1000)
      val cleanupHook = getShutdownHooks.find(isTempFileCleanupHook)
      cleanupHook shouldBe defined

      val files = cleanupHook.get.asInstanceOf[TempFileCleanup].filesToCleanup

      assert(files.size == 1)
      assert(files.head.contains(tempFile))
      cleanupHook.get.run()
      // Remove the hook so it doesn't show up in the list of shutdown hooks for other tests
      Runtime.getRuntime.removeShutdownHook(cleanupHook.get)
    }

    "clean up temporary files on finish" in {
      val tempFile = Files.createTempDirectory("scalding-execution-test").toFile.getAbsolutePath
      val testData = List("a", "b", "c")

      val ex = ExecutionTestJobs.writeExecutionWithTempFile(tempFile, testData)
      val onFinish = Execution.withConfig(ex)(_.setExecutionCleanupOnFinish(true))
      onFinish.shouldSucceedHadoop()

      // This is hacky, but there's a small chance that the cleanup thread has not finished
      // running by the time we check below
      // A small sleep like this appears to be sufficient to ensure we can see it
      Thread.sleep(1000)
      val f = new File(tempFile)
      def allChildren(f: File): List[File] =
        if (f.isDirectory) f.listFiles().toList.flatMap(allChildren(_))
        else List(f)

      assert(allChildren(f).isEmpty, f.toString)
    }

    "clean up temporary files on exit with a zip" in {
      val tempFileOne = Files.createTempDirectory("scalding-execution-test").toFile.getAbsolutePath
      val tempFileTwo = Files.createTempDirectory("scalding-execution-test").toFile.getAbsolutePath
      val testDataOne = List("a", "b", "c")
      val testDataTwo = List("x", "y", "z")
      getShutdownHooks.foreach { hook: Thread =>
        isTempFileCleanupHook(hook) should be(false)
      }

      ExecutionTestJobs.writeExecutionWithTempFile(tempFileOne, testDataOne)
        .zip(ExecutionTestJobs.writeExecutionWithTempFile(tempFileTwo, testDataTwo))
        .shouldSucceedHadoop()

      // This is hacky, but there's a small chance that the new cleanup hook isn't registered by the time we get here
      // A small sleep like this appears to be sufficient to ensure we can see it
      Thread.sleep(1000)
      val cleanupHook = getShutdownHooks.find(isTempFileCleanupHook)
      cleanupHook shouldBe defined

      val files = cleanupHook.get.asInstanceOf[TempFileCleanup].filesToCleanup

      assert(files.size == 2)
      assert(files.head.contains(tempFileOne) || files.head.contains(tempFileTwo))
      assert(files(1).contains(tempFileOne) || files(1).contains(tempFileTwo))
      cleanupHook.get.run()
      // Remove the hook so it doesn't show up in the list of shutdown hooks for other tests
      Runtime.getRuntime.removeShutdownHook(cleanupHook.get)
    }

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
      res.shouldSucceed()
      assert((first, second, third) == (1, 1, 1))
    }
    "zip does not duplicate counters" in {
      val c1 = Execution.withId { implicit uid =>
        val stat = Stat("test")
        val e1 = TypedPipe.from(0 until 100).map { x =>
          stat.inc
          x
        }
          .writeExecution(source.NullSink)

        e1.zip(e1)
      }
        .getCounters.map { case (_, c) => c("test") }

      val c2 = Execution.withId { implicit uid =>
        val stat = Stat("test")
        val e2 = TypedPipe.from(0 until 100).map { x =>
          stat.inc
          x
        }
          .writeExecution(source.NullSink)

        e2.flatMap(Execution.from(_)).zip(e2)
      }
        .getCounters.map { case (_, c) => c("test") }

      c1.shouldSucceed() should ===(100)
      c2.shouldSucceed() should ===(100)
    }
    "zip does not duplicate pure counters" in {
      val c1 = {
        val e1 = TypedPipe.from(0 until 100)
          .tallyAll("scalding", "test")
          .writeExecution(source.NullSink)

        e1.zip(e1)
          .getCounters.map { case (_, c) =>
            println(c.toMap)
            c(("test", "scalding"))
          }
      }

      val c2 = {
        val e2 = TypedPipe.from(0 until 100)
          .tallyAll("scalding", "test")
          .writeExecution(source.NullSink)

        e2.flatMap(Execution.from(_)).zip(e2)
          .getCounters.map { case (_, c) =>
            println(c.toMap)
            c(("test", "scalding"))
          }
      }

      c1.shouldSucceed() should ===(100)
      c2.shouldSucceed() should ===(100)
    }

    "Running a large loop won't exhaust boxed instances" in {
      var timesEvaluated = 0
      import com.twitter.scalding.serialization.macros.impl.BinaryOrdering._
      // Attempt to use up 4 boxed classes for every execution
      def baseExecution(idx: Int): Execution[Unit] = TypedPipe.from(0 until 1000).map(_.toShort).flatMap { i =>
        timesEvaluated += 1
        List((i, i), (i, i))
      }.sumByKey.map {
        case (k, v) =>
          (k.toInt, v)
      }.sumByKey.map {
        case (k, v) =>
          (k.toLong, v)
      }.sumByKey.map {
        case (k, v) =>
          (k.toString, v)
      }.sumByKey.map {
        case (k, v) =>
          (MyCustomType(k), v)
      }.sumByKey.writeExecution(TypedTsv(s"/tmp/asdf_${idx}"))

      implicitly[OrderedSerialization[MyCustomType]] match {
        case mos: MacroEqualityOrderedSerialization[_] => assert(mos.uniqueId == "com.twitter.scalding.MyCustomType")
        case _ => sys.error("Ordered serialization should have been the MacroEqualityOrderedSerialization for this test")
      }
      def executionLoop(idx: Int): Execution[Unit] = {
        if (idx > 0)
          baseExecution(idx).flatMap(_ => executionLoop(idx - 1))
        else
          Execution.unit
      }

      executionLoop(55).shouldSucceed()
      assert(timesEvaluated == 55 * 1000, "Should run the 55 execution loops for 1000 elements")
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

      res.shouldSucceed()
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

      res.shouldSucceed()
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

      res.shouldSucceed()
      assert(timesEvaluated == 1000, "Should share the common sub section of the graph when we zip two write Executions and then flatmap")
    }

    "Ability to do isolated caches so we don't exhaust memory" in {

      def memoryWastingExecutionGenerator(id: Int): Execution[Array[Long]] = Execution.withNewCache(Execution.from(id).flatMap{ idx =>
        Execution.from(Array.fill(4000000)(idx.toLong))
      })

      def writeAll(numExecutions: Int): Execution[Unit] = {
        if (numExecutions > 0) {
          memoryWastingExecutionGenerator(numExecutions).flatMap { _ =>
            writeAll(numExecutions - 1)
          }
        } else {
          Execution.from(())
        }
      }

      writeAll(400).shouldSucceed()
    }
    "handle failure" in {
      val result = Execution.withParallelism(Seq(Execution.failed(new Exception("failed"))), 1)

      result.shouldFail()
    }

    "handle an error running in parallel" in {
      val executions = Execution.failed(new Exception("failed")) :: 0.to(10).map(i => Execution.from[Int](i)).toList

      val result = Execution.withParallelism(executions, 3)

      result.shouldFail()
    }

    "run in parallel" in {
      val executions = 0.to(10).map(i => Execution.from[Int](i)).toList

      val result = Execution.withParallelism(executions, 3)

      assert(result.shouldSucceed() == 0.to(10).toSeq)
    }

    "block correctly" in {
      var seen = 0
      def updateSeen(idx: Int): Unit = {
        assert(seen === idx)
        seen += 1
      }

      val executions = 0.to(10).map{ i =>
        Execution
          .from[Int](i)
          .map{ i => Thread.sleep(10 - i); i }
          .onComplete(t => updateSeen(t.get))
      }.toList.reverse

      val result = Execution.withParallelism(executions, 1)

      assert(result.shouldSucceed() == 0.to(10).reverse)
    }

    "can hashCode, compare, and run a long sequence" in {
      val execution = Execution.sequence((1 to 100000).toList.map(Execution.from(_)))
      assert(execution.hashCode == execution.hashCode)

      assert(execution == execution)

      assert(execution.shouldSucceed() == (1 to 100000).toList)
    }

    "caches a withId Execution computation" in {
      var called = false
      val execution = Execution.withId { id =>
        assert(!called)
        called = true
        Execution.from("foobar")
      }

      val doubleExecution = execution.zip(execution)

      assert(doubleExecution.shouldSucceed() == ("foobar", "foobar"))
      assert(called)
    }

    "maintains equality and hashCode after reconstruction" when {
      // Make two copies of these.  Comparison by reference
      // won't match between the two.
      val futureF = { _: ConcurrentExecutionContext => Future.successful(10) }
      val futureF2 = { _: ConcurrentExecutionContext => Future.successful(10) }
      val fnF = { (_: Config, _: Mode) => null }
      val fnF2 = { (_: Config, _: Mode) => null }
      val withIdF = { _: UniqueID => Execution.unit }
      val withIdF2 = { _: UniqueID => Execution.unit }
      val mapF = { _: Int => 12 }
      val mapF2 = { _: Int => 12 }

      def reconstructibleLaws[T](ex: => Execution[T], ex2: Execution[T]): Unit = {
        assert(ex == ex)
        assert(ex.hashCode == ex.hashCode)
        assert(ex != ex2)
      }

      "Execution.fromFuture" in {
        reconstructibleLaws(Execution.fromFuture(futureF), Execution.fromFuture(futureF2))
      }

      "Execution.fromFn" in {
        reconstructibleLaws(Execution.fromFn(fnF), Execution.fromFn(fnF2))
      }

      "Execution.withId" in {
        reconstructibleLaws(Execution.withId(withIdF), Execution.withId(withIdF2))
      }

      "Execution#map" in {
        reconstructibleLaws(
          Execution.fromFuture(futureF).map(mapF),
          Execution.fromFuture(futureF).map(mapF2))
      }

      "Execution.zip" in {
        reconstructibleLaws(
          Execution.zip(Execution.fromFuture(futureF2), Execution.withId(withIdF)),
          Execution.zip(Execution.fromFuture(futureF2), Execution.withId(withIdF2)))
      }

      "Execution.sequence" in {
        reconstructibleLaws(
          Execution.sequence(Seq(
            Execution.fromFuture(futureF),
            Execution.withId(withIdF),
            Execution.fromFuture(futureF2).map(mapF))),
          Execution.sequence(Seq(
            Execution.fromFuture(futureF),
            Execution.withId(withIdF),
            Execution.fromFn(fnF))))
      }
    }

    "Has consistent hashCode and equality for mutable" when {
      // These cases are a bit convoluted, but we still
      // want equality to be consistent
      trait MutableX[T] {
        protected var x: Int
        def setX(newX: Int): Unit = { x = newX }
        def makeExecution: Execution[T]
      }

      case class FromFutureMutable(var x: Int = 0) extends Function1[ConcurrentExecutionContext, Future[Int]] with MutableX[Int] {
        def apply(context: ConcurrentExecutionContext) = Future.successful(x)
        def makeExecution = Execution.fromFuture(this)
      }
      case class FromFnMutable(var x: Int = 0) extends Function2[Config, Mode, Null] with MutableX[Unit] {
        def apply(config: Config, mode: Mode) = null
        def makeExecution = Execution.fromFn(this)
      }
      case class WithIdMutable(var x: Int = 0) extends Function1[UniqueID, Execution[Int]] with MutableX[Int] {
        def apply(id: UniqueID) = Execution.fromFuture(FromFutureMutable(x))
        def makeExecution = Execution.withId(this)
      }
      val mapFunction = { x: Int => x * x }
      case class MapMutable(var x: Int = 0) extends MutableX[Int] {
        val m = FromFutureMutable(x)
        override def setX(newX: Int) = {
          x = newX
          m.setX(x)
        }
        def makeExecution = m.makeExecution.map(mapFunction)
      }
      case class ZipMutable(var x: Int = 0) extends MutableX[(Int, Int)] {
        val m1 = FromFutureMutable(x)
        val m2 = WithIdMutable(x)
        override def setX(newX: Int) = {
          x = newX
          m1.setX(x)
          m2.setX(x + 20)
        }
        def makeExecution = m1.makeExecution.zip(m2.makeExecution)
      }
      case class SequenceMutable(var x: Int = 0) extends MutableX[Seq[Int]] {
        val m1 = FromFutureMutable(x)
        val m2 = WithIdMutable(x)
        override def setX(newX: Int) = {
          x = newX
          m1.setX(x)
          m2.setX(x * 3)
        }
        def makeExecution = Execution.sequence(Seq(m1.makeExecution, m2.makeExecution))
      }

      def mutableLaws[T, U <: MutableX[T]](
        mutableGen: => U,
        expectedOpt: Option[Int => T] = None): Unit = {
        expectedOpt.foreach { expected =>
          require(expected(10) != expected(20))
        }
        def validate(ex: Execution[T], seed: Int): Unit = {
          expectedOpt.foreach { expected =>
            assert(ex.shouldSucceed() == expected(seed))
          }
        }

        val mutable1 = mutableGen
        mutable1.setX(10)
        val ex1 = mutable1.makeExecution

        val mutable2 = mutableGen
        mutable2.setX(10)
        val ex2 = mutable2.makeExecution

        assert(ex1 == ex2)
        assert(ex1.hashCode == ex2.hashCode)

        validate(ex1, 10)
        validate(ex2, 10)

        mutable2.setX(20)
        // We may have the same hashCode still, but we don't need to
        assert(ex1 != ex2)
        validate(ex2, 20)

        val mutable3 = mutableGen
        mutable3.setX(20)
        val ex3 = mutable3.makeExecution

        assert(ex1 != ex3)
        validate(ex3, 20)

        mutable3.setX(10)
        if (ex1 == ex3) {
          // If they are made equal again, the hashCodes must match
          assert(ex1.hashCode == ex3.hashCode)
        }
        validate(ex3, 10)
      }

      "Execution.fromFuture" in {
        mutableLaws(FromFutureMutable(), Some({ x: Int => x }))
      }

      "Execution.fromFn" in {
        mutableLaws(FromFnMutable(), Option.empty[Int => Unit])
      }

      "Execution.withId" in {
        mutableLaws(WithIdMutable(), Some({ x: Int => x }))
      }

      "Execution#map" in {
        mutableLaws(MapMutable(), Some({ x: Int => x * x }))
      }

      "Execution#zip" in {
        mutableLaws(ZipMutable(), Some({ x: Int => (x, x + 20) }))
      }

      "Execution.sequence" in {
        mutableLaws(SequenceMutable(), Some({ x: Int => Seq(x, x * 3) }))
      }
    }
  }

  "Simple jobs" should {
    "convert to Execution and run" in {
      val ex = Job.toExecutionFromClass(classOf[NormalJobToExecutionTestJob], Execution.failed(new Exception("couldn't run")))
      val res = ex.waitFor(Config.empty, Local(true))
      assert(res.isSuccess)
    }
    "convert ExecutionJob to Execution" in {
      val test = JobTest(new WordCountEc(_))
        .arg("input", "in")
        .arg("output", "out")
        .source(TextLine("in"), List((0, "hello world"), (1, "goodbye world")))
        .typedSink(TypedTsv[(String, Long)]("out")) { outBuf =>
          outBuf.toMap shouldBe Map("hello" -> 1L, "world" -> 2L, "goodbye" -> 1L)
        }
      val ex = Job.toExecutionFromClass(classOf[WordCountEc], Execution.failed(new Exception("oh no")))
      val check =
        for {
          _ <- ex
          mode <- Execution.getMode
          _ = test.postRunChecks(mode)
        } yield ()

      val conf = Config.empty.setArgs(test.getArgs)
      val mode = test.getTestMode(useHadoop = false)
      assert(check.waitFor(conf, mode).isSuccess)
    }
  }

  "toIterableExecution" should {
    "work in TypedSource" in {
      val workingDir = System.getProperty("user.dir")
      val job = TypedPipe.from(TextLine(workingDir + "/../tutorial/data/hello.txt")).toIterableExecution
      assert(job.waitFor(Config.empty, Local(true)).get.toList == List("Hello world", "Goodbye world"))
    }
    "work in a mapped TypedSource" in {
      val workingDir = System.getProperty("user.dir")
      val job = TypedPipe.from(TextLine(workingDir + "/../tutorial/data/hello.txt")).map(_.size).toIterableExecution
      assert(job.waitFor(Config.empty, Local(true)).get.toList == List("Hello world", "Goodbye world").map(_.size))
    }
  }
}
