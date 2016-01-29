package com.twitter.scalding.platform

import cascading.flow.FlowStep
import cascading.flow.planner.BaseFlowStep
import cascading.pipe.joiner.{ JoinerClosure, InnerJoin }
import cascading.tuple.Tuple

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import java.util.{ Iterator => JIterator }
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.{ Ignore, Matchers, WordSpec }
import org.slf4j.{ LoggerFactory, Logger }
import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.math.Ordering

/**
 * This trait includes tests which only make sense on platforms where each (or most) Cascading
 * step is translated as an individual job on the underlying execution fabric
 * (e.g. Hadoop MAPREDUCE)
 *
 *
 */
trait StepwisePlatformTest extends PlatformTest {
  import ConfigBridge._

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

}
