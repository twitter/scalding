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

import cascading.flow.FlowStep
import cascading.flow.planner.BaseFlowStep
import cascading.pipe.joiner.{ JoinerClosure, InnerJoin }
import cascading.tuple.Tuple

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import java.util.{ Iterator => JIterator }
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.{ Matchers, WordSpec }
import org.slf4j.{ LoggerFactory, Logger }
import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.math.Ordering

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
  implicit val stringOrdSer = new StringOrderedSerialization()
  implicit val stringTup2OrdSer = new OrderedSerialization2(stringOrdSer, stringOrdSer)
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

class TypedPipeWithDescriptionJob(args: Args) extends Job(args) {
  TypedPipe.from[String](List("word1", "word1", "word2"))
    .withDescription("map stage - assign words to 1")
    .map { w => (w, 1L) }
    .group
    .withDescription("reduce stage - sum")
    .sum
    .withDescription("write")
    .write(TypedTsv[(String, Long)]("output"))
}

class TypedPipeJoinWithDescriptionJob(args: Args) extends Job(args) {
  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val y = TypedPipe.from[(Int, String)](List((1, "first")))
  val z = TypedPipe.from[(Int, Boolean)](List((2, true))).group

  x.hashJoin(y) // this triggers an implicit that somehow pushes the line number to the next one
    .withDescription("hashJoin")
    .leftJoin(z)
    .withDescription("leftJoin")
    .values
    .write(TypedTsv[((Int, String), Option[Boolean])]("output"))
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

object OrderedSerializationTest {
  implicit val genASGK = Arbitrary {
    for {
      ts <- Arbitrary.arbitrary[Long]
      b <- Gen.nonEmptyListOf(Gen.alphaNumChar).map (_.mkString)
    } yield NestedCaseClass(RichDate(ts), (b, b))
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get
  val data = sample[List[NestedCaseClass]].take(1000)
}

case class NestedCaseClass(day: RichDate, key: (String, String))

class ComplexJob(input: List[NestedCaseClass], args: Args) extends Job(args) {
  implicit def primitiveOrderedBufferSupplier[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]

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
  implicit def primitiveOrderedBufferSupplier[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]

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
    stat.inc

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
    stat.inc

    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    valuesIter.map({ case (a, b) => s"$a:$b" })
  }).toTypedPipe.write(TypedTsv[(String, String)]("output"))
}

// Keeping all of the specifications in the same tests puts the result output all together at the end.
// This is useful given that the Hadoop MiniMRCluster and MiniDFSCluster spew a ton of logging.
class PlatformTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  import ConfigBridge._

  "An InAndOutTest" should {
    val inAndOut = Seq("a", "b", "c")

    "reading then writing shouldn't change the data" in {
      HadoopPlatformJobTest(new InAndOutJob(_), cluster)
        .source("input", inAndOut)
        .sink[String]("output") { _.toSet shouldBe (inAndOut.toSet) }
        .run
    }
  }

  "A TinyJoinAndMergeJob" should {
    import TinyJoinAndMergeJob._

    "merge and joinWithTiny shouldn't duplicate data" in {
      HadoopPlatformJobTest(new TinyJoinAndMergeJob(_), cluster)
        .source(peopleInput, peopleData)
        .source(messageInput, messageData)
        .sink(output) { _.toSet shouldBe (outputData.toSet) }
        .run
    }
  }

  "A TsvNoCacheJob" should {
    import TsvNoCacheJob._

    "Writing to a tsv in a flow shouldn't effect the output" in {
      HadoopPlatformJobTest(new TsvNoCacheJob(_), cluster)
        .source(dataInput, data)
        .sink(typedThrowAwayOutput) { _.toSet should have size 4 }
        .sink(typedRealOutput) { _.map{ f: Float => (f * 10).toInt }.toList shouldBe (outputData.map{ f: Float => (f * 10).toInt }.toList) }
        .run
    }
  }

  "A multiple group by job" should {
    import MultipleGroupByJobData._

    "do some ops and not stamp on each other ordered serializations" in {
      HadoopPlatformJobTest(new MultipleGroupByJob(_), cluster)
        .source[String]("input", data)
        .sink[String]("output") { _.toSet shouldBe data.map(_.toString).toSet }
        .run
    }

  }

  "A TypedPipeForceToDiskWithDescriptionPipe" should {
    "have a custom step name from withDescription" in {

      HadoopPlatformJobTest(new TypedPipeForceToDiskWithDescriptionJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          val firstStep = steps.filter(_.getName.startsWith("(1/2)"))
          val secondStep = steps.filter(_.getName.startsWith("(2/2)"))
          val lab1 = firstStep.map(_.getConfigValue(Config.StepDescriptions))
          lab1 should have size 1
          lab1(0) should include ("write words to disk")
          val lab2 = secondStep.map(_.getConfigValue(Config.StepDescriptions))
          lab2 should have size 1
          lab2(0) should include ("output frequency by length")
        }
        .run
    }
  }

  "A TypedPipeJoinWithDescriptionPipe" should {
    "have a custom step name from withDescription" in {
      HadoopPlatformJobTest(new TypedPipeJoinWithDescriptionJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1
          val firstStepDescs = steps.headOption.map(_.getConfigValue(Config.StepDescriptions)).getOrElse("")
          val firstStepDescSet = firstStepDescs.split(",").map(_.trim).toSet

          val expected = Set(149, 151, 152, 155, 156).map { i =>
            s"com.twitter.scalding.platform.TypedPipeJoinWithDescriptionJob.<init>(PlatformTest.scala:$i)"
          } ++ Seq("leftJoin", "hashJoin")

          firstStepDescSet should equal(expected)
          steps.map(_.getConfigValue(Config.StepDescriptions)).foreach(s => info(s))
        }
        .run
    }
  }

  "A TypedPipeWithDescriptionPipe" should {
    "have a custom step name from withDescription" in {
      HadoopPlatformJobTest(new TypedPipeWithDescriptionJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          val expectedDescs = Set("map stage - assign words to 1",
            "reduce stage - sum",
            "write") ++
            Seq(138, 139, 141, 142, 143).map(
              linenum => s"com.twitter.scalding.platform.TypedPipeWithDescriptionJob.<init>(PlatformTest.scala:${linenum})")

          val foundDescs = steps.map(_.getConfigValue(Config.StepDescriptions).split(",").map(_.trim).toSet)
          foundDescs should have size 1

          foundDescs.head should equal(expectedDescs)
          //steps.map(_.getConfig.get(Config.StepDescriptions)).foreach(s => info(s))
        }
        .run
    }
  }

  "A IterableSource" should {
    import IterableSourceDistinctJob._

    "distinct properly from normal data" in {
      HadoopPlatformJobTest(new NormalDistinctJob(_), cluster)
        .source[String]("input", data ++ data ++ data)
        .sink[String]("output") { _.toList shouldBe data }
        .run
    }

    "distinctBy(identity) properly from a list in memory" in {
      HadoopPlatformJobTest(new IterableSourceDistinctIdentityJob(_), cluster)
        .sink[String]("output") { _.toList shouldBe data }
        .run
    }

    "distinct properly from a list" in {
      HadoopPlatformJobTest(new IterableSourceDistinctJob(_), cluster)
        .sink[String]("output") { _.toList shouldBe data }
        .run
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
        .run
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
        .run
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
        .run
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
        .run
    }
  }

}
