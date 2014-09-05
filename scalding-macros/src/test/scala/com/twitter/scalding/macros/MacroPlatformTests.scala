package com.twitter.scalding.macros

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.scalding.macros.impl.MacroGenerated
import com.twitter.scalding.platform.{ HadoopPlatformTest, HadoopPlatformJobTest }

case class InA(x: Int, y: String, z: Double)
case class OutB(y: String, z: Double, x: Int)

object InAOutBJob {
  import MacroImplicits._

  val input = List(
    InA(1, "one", 1.8),
    InA(-11, "blahjkdsf", -3.3488))

  val output = input.map { case InA(x, y, z) => OutB(y + y, z, x * x) }

  val inTypedTsv = TypedTsv[InA]("input")
  val outTypedTsv = TypedTsv[OutB]("output")
}
class InAOutBJob(args: Args) extends Job(args) {
  import MacroImplicits._
  import InAOutBJob._

  TypedPipe.from(inTypedTsv)
    .map { case InA(x, y, z) => OutB(y + y, z, x * x) }
    .write(outTypedTsv)
}

// This tests all of the logic we can test without running the full integration tests
class MacroJobTest extends WordSpec with Matchers {
  import InAOutBJob._
  import MacroImplicits._

  "An JobTest InAOutBJob" should {
    "use the macro generated setters and converters" in {
      inTypedTsv.conv shouldBe a[MacroGenerated]
      inTypedTsv.tset shouldBe a[MacroGenerated]
      outTypedTsv.conv shouldBe a[MacroGenerated]
      outTypedTsv.tset shouldBe a[MacroGenerated]
    }

    "evaluate as expected" in {
      JobTest(new InAOutBJob(_))
        .source(inTypedTsv, input)
        .typedSink(outTypedTsv) { _.toSet shouldBe output.toSet }
        .run
        .runHadoop
        .finish
    }
  }
}

class MacroPlatformTests extends WordSpec with Matchers with HadoopPlatformTest {
  import InAOutBJob._

  "A platform InAOutBJob" should {
    "handle case classes properly" in {
      HadoopPlatformJobTest(new InAOutBJob(_), cluster)
        .source(inTypedTsv, input)
        .sink(outTypedTsv) { _.toSet shouldBe output.toSet }
        .run
    }
  }
}
