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
package com.twitter.scalding.platform

import java.util.{ Iterator => JIterator }

import cascading.flow.FlowException
import cascading.pipe.joiner.{ InnerJoin, JoinerClosure }
import cascading.scheme.Scheme
import cascading.scheme.hadoop.{ TextLine => CHTextLine }
import cascading.tap.Tap
import cascading.tuple.{ Fields, Tuple }
import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.source.{ FixedTypedText, NullSink, TypedText }
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.{ Matchers, WordSpec }

import scala.collection.JavaConverters._
import scala.language.experimental.macros

class InAndOutJob(args: Args) extends Job(args) {
  Tsv("input").read.write(Tsv("output"))
}

object TinyJoinAndMergeJob {
  val peopleInput = TypedTsv[Int]("input1")
  val peopleData = List(1, 2, 3, 4)

  val messageInput = TypedTsv[Int]("input2")
  val messageData = List(1, 2, 3)

  val output = TypedTsv[(Int, Int)]("output")
  val outputData = List((1, 2), (2, 2), (3, 2), (4, 1))
}

class TinyJoinAndMergeJob(args: Args) extends Job(args) {
  import TinyJoinAndMergeJob._

  val people = peopleInput.read.mapTo(0 -> 'id) { v: Int => v }

  val messages = messageInput.read
    .mapTo(0 -> 'id) { v: Int => v }
    .joinWithTiny('id -> 'id, people)

  (messages ++ people).groupBy('id) { _.size('count) }.write(output)
}

object TsvNoCacheJob {
  val dataInput = TypedTsv[String]("fakeInput")
  val data = List("-0.2f -0.3f -0.5f", "-0.1f", "-0.5f")

  val throwAwayOutput = Tsv("output1")
  val typedThrowAwayOutput = TypedTsv[Float]("output1")
  val realOuput = Tsv("output2")
  val typedRealOutput = TypedTsv[Float]("output2")
  val outputData = List(-0.5f, -0.2f, -0.3f, -0.1f).sorted
}
class TsvNoCacheJob(args: Args) extends Job(args) {
  import TsvNoCacheJob._
  dataInput.read
    .flatMap(new cascading.tuple.Fields(Integer.valueOf(0)) -> 'word){ line: String => line.split("\\s") }
    .groupBy('word){ group => group.size }
    .mapTo('word -> 'num) { (w: String) => w.toFloat }
    .write(throwAwayOutput)
    .groupAll { _.sortBy('num) }
    .write(realOuput)
}

object IterableSourceDistinctJob {
  val data = List("a", "b", "c")
}

class IterableSourceDistinctJob(args: Args) extends Job(args) {
  import IterableSourceDistinctJob._

  TypedPipe.from(data ++ data ++ data).distinct.write(TypedTsv("output"))
}

class IterableSourceDistinctIdentityJob(args: Args) extends Job(args) {
  import IterableSourceDistinctJob._

  TypedPipe.from(data ++ data ++ data).distinctBy(identity).write(TypedTsv("output"))
}

class NormalDistinctJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[String]("input")).distinct.write(TypedTsv("output"))
}

object MultipleGroupByJobData {
  val data: List[String] = {
    val rnd = new scala.util.Random(22)
    (0 until 20).map { _ => rnd.nextLong.toString }.toList
  }.distinct
}

class MultipleGroupByJob(args: Args) extends Job(args) {
  import com.twitter.scalding.serialization._
  import MultipleGroupByJobData._
  implicit val stringOrdSer: OrderedSerialization[String] = new StringOrderedSerialization()
  implicit val stringTup2OrdSer: OrderedSerialization[(String, String)] = new OrderedSerialization2(stringOrdSer, stringOrdSer)
  val otherStream = TypedPipe.from(data).map{ k => (k, k) }.group

  TypedPipe.from(data)
    .map{ k => (k, 1L) }
    .group[String, Long](implicitly, stringOrdSer)
    .sum
    .map {
      case (k, _) =>
        ((k, k), 1L)
    }
    .sumByKey[(String, String), Long](implicitly, stringTup2OrdSer, implicitly)
    .map(_._1._1)
    .map { t =>
      (t.toString, t)
    }
    .group
    .leftJoin(otherStream)
    .map(_._1)
    .write(TypedTsv("output"))

}

class TypedPipeHashJoinWithForceToDiskJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)

  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val y = TypedPipe.from[(Int, String)](List((1, "first")))

  //trivial transform and forceToDisk on the rhs
  val yMap = y.map(p => (p._1, p._2.toUpperCase)).forceToDisk

  x.hashJoin(yMap)
    .withDescription("hashJoin")
    .write(TypedTsv[(Int, (Int, String))]("output"))
}

class TypedPipeHashJoinWithForceToDiskFilterJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)

  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val y = TypedPipe.from[(Int, String)](List((1, "first")))

  //trivial transform and forceToDisk followed by filter on rhs
  val yFilter = y.map(p => (p._1, p._2.toUpperCase)).forceToDisk.filter(p => p._1 == 1)

  x.hashJoin(yFilter)
    .withDescription("hashJoin")
    .write(TypedTsv[(Int, (Int, String))]("output"))
}

class TypedPipeHashJoinWithForceToDiskWithComplete(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)

  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val y = TypedPipe.from[(Int, String)](List((1, "first")))

  //trivial transform and forceToDisk followed by WithComplete on rhs
  val yComplete = y.map(p => (p._1, p._2.toUpperCase)).forceToDisk.onComplete(() => println("step complete"))

  x.hashJoin(yComplete)
    .withDescription("hashJoin")
    .write(TypedTsv[(Int, (Int, String))]("output"))
}

class TypedPipeHashJoinWithForceToDiskMapJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, false)
  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val y = TypedPipe.from[(Int, String)](List((1, "first")))

  //trivial transform and forceToDisk followed by map on rhs
  val yMap = y.map(p => (p._1, p._2.toUpperCase)).forceToDisk.map(p => (p._1, p._2.toLowerCase))

  x.hashJoin(yMap)
    .withDescription("hashJoin")
    .write(TypedTsv[(Int, (Int, String))]("output"))
}

class TypedPipeHashJoinWithForceToDiskMapWithAutoForceJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)
  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val y = TypedPipe.from[(Int, String)](List((1, "first")))

  //trivial transform and forceToDisk followed by map on rhs
  val yMap = y.map(p => (p._1, p._2.toUpperCase)).forceToDisk.map(p => (p._1, p._2.toLowerCase))

  x.hashJoin(yMap)
    .withDescription("hashJoin")
    .write(TypedTsv[(Int, (Int, String))]("output"))
}

class TypedPipeHashJoinWithGroupByJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)

  val x = TypedPipe.from[(String, Int)](Tsv("input1", ('x1, 'y1)), Fields.ALL)
  val y = Tsv("input2", ('x2, 'y2))

  val yGroup = y.groupBy('x2){ p => p }
  val yTypedPipe = TypedPipe.from[(String, Int)](yGroup, Fields.ALL)

  x.hashJoin(yTypedPipe)
    .withDescription("hashJoin")
    .write(TypedTsv[(String, (Int, Int))]("output"))
}

class TypedPipeHashJoinWithCoGroupJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)

  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val in0 = Tsv("input0").read.mapTo((0, 1) -> ('x0, 'a)) { input: (Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0, 1) -> ('x1, 'b)) { input: (Int, Int) => input }

  val coGroupPipe = in0.coGroupBy('x0) {
    _.coGroup('x1, in1, OuterJoinMode)
  }

  val coGroupTypedPipe = TypedPipe.from[(Int, Int, Int)](coGroupPipe, Fields.ALL)
  val coGroupTuplePipe = coGroupTypedPipe.map{ case (a, b, c) => (a, (b, c)) }
  x.hashJoin(coGroupTuplePipe)
    .withDescription("hashJoin")
    .write(TypedTsv[(Int, (Int, (Int, Int)))]("output"))
}

class TypedPipeHashJoinWithEveryJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)

  val x = TypedPipe.from[(Int, String)](Tsv("input1", ('x1, 'y1)), Fields.ALL)
  val y = Tsv("input2", ('x2, 'y2)).groupBy('x2) {
    _.foldLeft('y2 -> 'y2)(0){ (b: Int, a: Int) => b + a }
  }

  val yTypedPipe = TypedPipe.from[(Int, Int)](y, Fields.ALL)
  x.hashJoin(yTypedPipe)
    .withDescription("hashJoin")
    .write(TypedTsv[(Int, (String, Int))]("output"))
}

class TypedPipeForceToDiskWithDescriptionJob(args: Args) extends Job(args) {
  val writeWords = {
    TypedPipe.from[String](List("word1 word2", "word1", "word2"))
      .withDescription("write words to disk")
      .flatMap { _.split("\\s+") }
      .forceToDisk
  }
  writeWords
    .groupBy(_.length)
    .withDescription("output frequency by length")
    .size
    .write(TypedTsv[(Int, Long)]("output"))
}

class GroupedLimitJobWithSteps(args: Args) extends Job(args) {
  val writeWords =
    TypedPipe.from[String](List("word1 word2", "word1", "word2"))
      .flatMap { _.split("\\s+") }
      .map{ k => k -> 1L }
      .sumByKey
      .limit(3)

  writeWords
    .groupBy(_._1)
    .head
    .keys
    .write(TypedTsv[String]("output1"))

  writeWords
    .groupBy(_._1)
    .head
    .keys
    .write(TypedTsv[String]("output2"))
}

object OrderedSerializationTest {
  implicit val genASGK: Arbitrary[NestedCaseClass] = Arbitrary {
    for {
      ts <- Arbitrary.arbitrary[Long]
      b <- Gen.nonEmptyListOf(Gen.alphaNumChar).map (_.mkString)
    } yield NestedCaseClass(RichDate(ts), (b, b))
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get
  val data = sample[List[NestedCaseClass]].take(1000)
}

case class NestedCaseClass(day: RichDate, key: (String, String))

// Need to define this in a separate companion object to work around Scala 2.12 compile issues
object OrderedSerializationImplicitDefs {
  implicit def primitiveOrderedBufferSupplier[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]
}

class ComplexJob(input: List[NestedCaseClass], args: Args) extends Job(args) {
  import OrderedSerializationImplicitDefs._

  val ds1 = TypedPipe.from(input).map(_ -> 1L).group.sorted.mapValueStream(_.map(_ * 2)).toTypedPipe.group

  val ds2 = TypedPipe.from(input).map(_ -> 1L).distinct.group

  ds2
    .keys
    .map(s => s.toString)
    .write(TypedTsv[String](args("output1")))

  ds2.join(ds1)
    .values
    .map(_.toString)
    .write(TypedTsv[String](args("output2")))
}

class ComplexJob2(input: List[NestedCaseClass], args: Args) extends Job(args) {
  import OrderedSerializationImplicitDefs._

  val ds1 = TypedPipe.from(input).map(_ -> (1L, "asfg"))

  val ds2 = TypedPipe.from(input).map(_ -> (1L, "sdf"))

  val execution = ds1.join(ds2).groupAll.size.values.toIterableExecution
  val r = Config.tryFrom(config).get
  execution.waitFor(r, mode).get

  ds1.map(_.toString).write(TypedTsv[String](args("output1")))
  ds2.map(_.toString).write(TypedTsv[String](args("output2")))
}

class CheckFlowProcessJoiner(uniqueID: UniqueID) extends InnerJoin {
  override def getIterator(joinerClosure: JoinerClosure): JIterator[Tuple] = {
    println("CheckFlowProcessJoiner.getItertor")

    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    super.getIterator(joinerClosure)
  }
}

class CheckForFlowProcessInFieldsJob(args: Args) extends Job(args) {
  val uniqueID = UniqueID.getIDFor(flowDef)
  val stat = Stat("joins")

  val inA = Tsv("inputA", ('a, 'b))
  val inB = Tsv("inputB", ('x, 'y))

  val p = inA.joinWithSmaller('a -> 'x, inB).map(('b, 'y) -> 'z) { args: (String, String) =>
    stat.inc()

    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    s"${args._1},${args._2}"
  }

  p.write(Tsv("output", ('b, 'y)))
}

class CheckForFlowProcessInTypedJob(args: Args) extends Job(args) {
  val uniqueID = UniqueID.getIDFor(flowDef)
  val stat = Stat("joins")

  val inA = TypedPipe.from(TypedTsv[(String, String)]("inputA"))
  val inB = TypedPipe.from(TypedTsv[(String, String)]("inputB"))

  inA.group.join(inB.group).forceToReducers.mapGroup((key, valuesIter) => {
    stat.inc()

    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    valuesIter.map({ case (a, b) => s"$a:$b" })
  }).toTypedPipe.write(TypedTsv[(String, String)]("output"))
}

case class BypassValidationSource(path: String) extends FixedTypedText[Int](TypedText.TAB, path) {
  override def validateTaps(mode: Mode): Unit = ()
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    (mode, readOrWrite) match {
      case (hdfsMode: Hdfs, Read) => new InvalidSourceTap(Seq(path))
      case _ => super.createTap(readOrWrite)
    }
}

class ReadPathJob(args: Args) extends Job(args) {
  TypedPipe
    .from(new BypassValidationSource(args.required("input")))
    .write(NullSink)
}

object PlatformTest {
  def setAutoForceRight(mode: Mode, autoForce: Boolean): Unit = {
    mode match {
      case h: HadoopMode =>
        val config = h.jobConf
        config.setBoolean(Config.HashJoinAutoForceRight, autoForce)
      case _ => ()
    }
  }
}

class TestTypedEmptySource extends FileSource with TextSourceScheme with Mappable[(Long, String)] with SuccessFileSource {
  override def hdfsPaths: Iterable[String] = Iterable.empty
  override def localPaths: Iterable[String] = Iterable.empty
  override def converter[U >: (Long, String)] =
    TupleConverter.asSuperConverter[(Long, String), U](implicitly[TupleConverter[(Long, String)]])
}

// Tests the scenario where you have no data present in the directory pointed to by a source typically
// due to the directory being empty (but for a _SUCCESS file)
// We test out that this shouldn't result in a Cascading planner error during {@link Job.buildFlow}
class EmptyDataJob(args: Args) extends Job(args) {
  TypedPipe.from(new TestTypedEmptySource)
    .map { case (offset, line) => line }
    .write(TypedTsv[String]("output"))
}

// Keeping all of the specifications in the same tests puts the result output all together at the end.
// This is useful given that the Hadoop MiniMRCluster and MiniDFSCluster spew a ton of logging.
class PlatformTest extends WordSpec with Matchers with HadoopSharedPlatformTest {

  "An InAndOutTest" should {
    val inAndOut = Seq("a", "b", "c")

    "reading then writing shouldn't change the data" in {
      HadoopPlatformJobTest(new InAndOutJob(_), cluster)
        .source("input", inAndOut)
        .sink[String]("output") { _.toSet shouldBe (inAndOut.toSet) }
        .run()
    }
  }

  "A TinyJoinAndMergeJob" should {
    import TinyJoinAndMergeJob._

    "merge and joinWithTiny shouldn't duplicate data" in {
      HadoopPlatformJobTest(new TinyJoinAndMergeJob(_), cluster)
        .source(peopleInput, peopleData)
        .source(messageInput, messageData)
        .sink(output) { _.toSet shouldBe (outputData.toSet) }
        .run()
    }
  }

  "A TsvNoCacheJob" should {
    import TsvNoCacheJob._

    "Writing to a tsv in a flow shouldn't effect the output" in {
      HadoopPlatformJobTest(new TsvNoCacheJob(_), cluster)
        .source(dataInput, data)
        .sink(typedThrowAwayOutput) { _.toSet should have size 4 }
        .sink(typedRealOutput) { _.map{ f: Float => (f * 10).toInt }.toList shouldBe (outputData.map{ f: Float => (f * 10).toInt }.toList) }
        .run()
    }
  }

  "A multiple group by job" should {
    import MultipleGroupByJobData._

    "do some ops and not stamp on each other ordered serializations" in {
      HadoopPlatformJobTest(new MultipleGroupByJob(_), cluster)
        .source[String]("input", data)
        .sink[String]("output") { _.toSet shouldBe data.map(_.toString).toSet }
        .run()
    }

  }

  "A TypedPipeForceToDiskWithDescriptionPipe" should {
    "have a custom step name from withDescription" in {
      HadoopPlatformJobTest(new TypedPipeForceToDiskWithDescriptionJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          val firstStep = steps.filter(_.getName.startsWith("(1/2"))
          val secondStep = steps.filter(_.getName.startsWith("(2/2"))
          val lab1 = firstStep.map(_.getConfig.get(Config.StepDescriptions))
          lab1 should have size 1
          lab1(0) should include ("write words to disk")
          val lab2 = secondStep.map(_.getConfig.get(Config.StepDescriptions))
          lab2 should have size 1
          lab2(0) should include ("output frequency by length")
        }
        .run()
    }
  }

  "A limit" should {
    "not fan out into consumers" in {
      // This covers a case where limit was being swept into a typed pipe factory
      // so each consumer was re-running the limit independently
      // which makes it usage unstable too.
      HadoopPlatformJobTest(new GroupedLimitJobWithSteps(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 4
        }
        .run()
    }
  }

  //also tests HashJoin behavior to verify that we don't introduce a forceToDisk as the RHS pipe is source Pipe
  "A TypedPipeJoinWithDescriptionPipe" should {
    "have a custom step name from withDescription and no extra forceToDisk steps on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeJoinWithDescriptionJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1
          val firstStep = steps.headOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          firstStep should include ("leftJoin")
          firstStep should include ("hashJoin")
          steps.map(_.getConfig.get(Config.StepDescriptions)).foreach(s => info(s))
        }
        .run()
    }
  }

  //expect two jobs - one for the map prior to the Checkpoint and one for the hashJoin
  "A TypedPipeHashJoinWithForceToDiskJob" should {
    "have a custom step name from withDescription and only one user provided forceToDisk on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithForceToDiskJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 2
          val secondStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          secondStep should include ("hashJoin")
        }
        .run()
    }
  }

  //expect 3 jobs - one extra compared to previous as there's a new forceToDisk added
  "A TypedPipeHashJoinWithForceToDiskFilterJob" should {
    "have a custom step name from withDescription and an extra forceToDisk due to a filter operation on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithForceToDiskFilterJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 3
          val lastStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          lastStep should include ("hashJoin")
        }
        .run()
    }
  }

  //expect two jobs - one for the map prior to the Checkpoint and one for the rest
  "A TypedPipeHashJoinWithForceToDiskWithComplete" should {
    "have a custom step name from withDescription and no extra forceToDisk due to with complete operation on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithForceToDiskWithComplete(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 2
          val lastStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          lastStep should include ("hashJoin")
        }
        .run()
    }
  }

  //expect two jobs - one for the map prior to the Checkpoint and one for the rest
  "A TypedPipeHashJoinWithForceToDiskMapJob" should {
    "have a custom step name from withDescription and no extra forceToDisk due to map (autoForce = false) on forceToDisk operation on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithForceToDiskMapJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 2
          val lastStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          lastStep should include ("hashJoin")
        }
        .run()
    }
  }

  //expect one extra job from the above - we end up performing a forceToDisk after the map
  "A TypedPipeHashJoinWithForceToDiskMapWithAutoForceJob" should {
    "have a custom step name from withDescription and an extra forceToDisk due to map (autoForce = true) on forceToDisk operation on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithForceToDiskMapWithAutoForceJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 3
          val lastStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          lastStep should include ("hashJoin")
        }
        .run()
    }
  }

  "A TypedPipeHashJoinWithGroupByJob" should {
    "have a custom step name from withDescription and no extra forceToDisk after groupBy on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithGroupByJob(_), cluster)
        .source(TypedTsv[(String, Int)]("input1"), Seq(("first", 45)))
        .source(TypedTsv[(String, Int)]("input2"), Seq(("first", 1), ("first", 2), ("first", 3), ("second", 1), ("second", 2)))
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 2
          val lastStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          lastStep should include ("hashJoin")
        }
        .run()
    }
  }

  "A TypedPipeHashJoinWithCoGroupJob" should {
    "have a custom step name from withDescription and no extra forceToDisk after coGroup + map on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithCoGroupJob(_), cluster)
        .source(TypedTsv[(Int, Int)]("input0"), List((0, 1), (1, 1), (2, 1), (3, 2)))
        .source(TypedTsv[(Int, Int)]("input1"), List((0, 1), (2, 5), (3, 2)))
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 2
          val lastStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          lastStep should include ("hashJoin")
        }
        .run()
    }
  }

  "A TypedPipeHashJoinWithEveryJob" should {
    "have a custom step name from withDescription and no extra forceToDisk after an Every on hashJoin's rhs" in {
      HadoopPlatformJobTest(new TypedPipeHashJoinWithEveryJob(_), cluster)
        .source(TypedTsv[(Int, String)]("input1"), Seq((1, "foo")))
        .source(TypedTsv[(Int, Int)]("input2"), Seq((1, 30), (1, 10), (1, 20), (2, 20)))
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 2
          val lastStep = steps.lastOption.map(_.getConfig.get(Config.StepDescriptions)).getOrElse("")
          lastStep should include ("hashJoin")
        }
        .run()
    }
  }

  "A TypedPipeWithDescriptionPipe" should {
    "have a custom step name from withDescription" in {
      HadoopPlatformJobTest(new TypedPipeWithDescriptionJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          val descs = List("map stage - assign words to 1",
            "reduce stage - sum",
            "write",
            // should see the .group and the .write show up as line numbers
            "com.twitter.scalding.platform.TypedPipeWithDescriptionJob.<init>(TestJobsWithDescriptions.scala:30)",
            "com.twitter.scalding.platform.TypedPipeWithDescriptionJob.<init>(TestJobsWithDescriptions.scala:34)")

          val foundDescs = steps.map(_.getConfig.get(Config.StepDescriptions))
          descs.foreach { d =>
            assert(foundDescs.size == 1)
            assert(foundDescs(0).contains(d))
          }
          //steps.map(_.getConfig.get(Config.StepDescriptions)).foreach(s => info(s))
        }
        .run()
    }
  }

  "A IterableSource" should {
    import IterableSourceDistinctJob._

    "distinct properly from normal data" in {
      HadoopPlatformJobTest(new NormalDistinctJob(_), cluster)
        .source[String]("input", data ++ data ++ data)
        .sink[String]("output") { _.toList shouldBe data }
        .run()
    }

    "distinctBy(identity) properly from a list in memory" in {
      HadoopPlatformJobTest(new IterableSourceDistinctIdentityJob(_), cluster)
        .sink[String]("output") { _.toList shouldBe data }
        .run()
    }

    "distinct properly from a list" in {
      HadoopPlatformJobTest(new IterableSourceDistinctJob(_), cluster)
        .sink[String]("output") { _.toList shouldBe data }
        .run()
    }
  }

  // If we support empty sources again in the future, update this test
  "An EmptyData source" should {
    "read from empty source and write to output without errors" in {
      val e = intercept[FlowException] {
        HadoopPlatformJobTest(new EmptyDataJob(_), cluster)
          .run()
      }
      assert(e.getCause.getClass === classOf[InvalidSourceException])
    }
  }

  import OrderedSerializationTest._
  "An Ordered Serialization" should {
    "A test job with a fork and join, had previously not had boxed serializations on all branches" in {
      val fn = (arg: Args) => new ComplexJob(data, arg)
      HadoopPlatformJobTest(fn, cluster)
        .arg("output1", "output1")
        .arg("output2", "output2")
        // Here we are just testing that we hit no exceptions in the course of this run
        // the previous issue would have caused OOM or other exceptions. If we get to the end
        // then we are good.
        .sink[String](TypedTsv[String]("output2")) { x => () }
        .sink[String](TypedTsv[String]("output1")) { x => () }
        .run()
    }

    "A test job with that joins then groupAll's should have its boxes setup correctly." in {
      val fn = (arg: Args) => new ComplexJob2(data, arg)
      HadoopPlatformJobTest(fn, cluster)
        .arg("output1", "output1")
        .arg("output2", "output2")
        // Here we are just testing that we hit no exceptions in the course of this run
        // the previous issue would have caused OOM or other exceptions. If we get to the end
        // then we are good.
        .sink[String](TypedTsv[String]("output2")) { x => () }
        .sink[String](TypedTsv[String]("output1")) { x => () }
        .run()
    }
  }

  "Methods called from a Joiner" should {
    "have access to a FlowProcess from a join in the Fields-based API" in {
      HadoopPlatformJobTest(new CheckForFlowProcessInFieldsJob(_), cluster)
        .source(TypedTsv[(String, String)]("inputA"), Seq(("1", "alpha"), ("2", "beta")))
        .source(TypedTsv[(String, String)]("inputB"), Seq(("1", "first"), ("2", "second")))
        .sink(TypedTsv[(String, String)]("output")) { _ =>
          // The job will fail with an exception if the FlowProcess is unavailable.
        }
        .inspectCompletedFlow({ flow =>
          flow.getFlowStats.getCounterValue(Stats.ScaldingGroup, "joins") shouldBe 2
        })
        .run()
    }

    "have access to a FlowProcess from a join in the Typed API" in {
      HadoopPlatformJobTest(new CheckForFlowProcessInTypedJob(_), cluster)
        .source(TypedTsv[(String, String)]("inputA"), Seq(("1", "alpha"), ("2", "beta")))
        .source(TypedTsv[(String, String)]("inputB"), Seq(("1", "first"), ("2", "second")))
        .sink[(String, String)](TypedTsv[(String, String)]("output")) { _ =>
          // The job will fail with an exception if the FlowProcess is unavailable.
        }
        .inspectCompletedFlow({ flow =>
          flow.getFlowStats.getCounterValue(Stats.ScaldingGroup, "joins") shouldBe 2
        })
        .run()
    }
  }

  "An InvalidSourceTap that gets past validation" should {
    "throw an InvalidSourceException" in {
      val result: FlowException = intercept[FlowException] {
        HadoopPlatformJobTest(new ReadPathJob(_), cluster)
          .arg("input", "/sploop/boop/doopity/doo/")
          .run()
      }

      assert(Option(result.getCause).exists(_.isInstanceOf[InvalidSourceException]))
    }
  }
}
