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
 * This trait includes tests which only make sense on platforms where each Flow is translated
 * as a single DAG onto the underlying execution fabric
 * (e.g. Tez, Flink but possibly not Spark)
 *
 */
trait DagwisePlatformTest extends PlatformTest {
  import ConfigBridge._

  "A TypedPipeForceToDiskWithDescriptionPipe" should {
    "have a custom step name from withDescription" in {

      HadoopPlatformJobTest(new TypedPipeForceToDiskWithDescriptionJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val labels = steps.head.getConfigValue(Config.StepDescriptions).split(",").map(_.trim).toSet
          labels.contains("write words to disk") should be(true)
          labels.contains("output frequency by length") should be(true)

          /* ".forceToDisk" may have an influence on Tez and other "whole DAG" processing engines but
            it should not cause a new Step
            
              (note: "partial DAG" engines where Cascading has to generate 1 or more steps
              depending on the Flow's exact topology may disagree about this; in which case
              make another trait from PlatformTest to model this and put the appropriate tests)
           */
        }
        .run
    }
  }

}
