package com.twitter.scalding.strategy;

import cascading.flow.FlowStepStrategy
import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, LocalCluster }
import org.apache.hadoop.mapred.JobConf
import org.specs._

object SimpleTest {
  val inSrc = TypedTsv[String]("input")
  val inData = List("Hello world", "Goodbye world")
  val out = TypedTsv[(String, Int)]("output")
  val correct = Map("hello" -> 1, "goodbye" -> 1, "world" -> 2)
}

class SimpleTest(args: Args) extends Job(args) {
  import SimpleTest._

  override def stepStrategy: Option[FlowStepStrategy[JobConf]] = {
    println("@> custom StepStrategy!")
    Some(new ReducerEstimator)
  }

  def tokenize(text: String): TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")

  TypedPipe.from(inSrc)
    .flatMap(tokenize)
    .map(_ -> 1)
    .group
    .sum
    .write(out)

}

class ReducerEstimatorTest extends Specification {

  import SimpleTest._

  "ReducerEstimator" should {
    val cluster = LocalCluster()
    doFirst { cluster.initialize() }

    "be runnable" in {
      HadoopPlatformJobTest(new SimpleTest(_), cluster)
        .source(inSrc, inData)
        .sink[(String, Int)](out)(_.toMap must_== correct)
        .run
    }

    doLast { cluster.shutdown() }
  }

}
