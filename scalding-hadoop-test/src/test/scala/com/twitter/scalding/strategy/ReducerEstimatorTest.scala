package com.twitter.scalding.strategy

import cascading.flow.FlowStepStrategy
import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, LocalCluster }
import org.apache.hadoop.mapred.JobConf
import org.specs._

object SimpleTest {

  val inSrc = TextLine(getClass.getResource("/hipster.txt").toString)
  val inScores = TypedTsv[(String, Double)](getClass.getResource("/scores.tsv").toString)
  val out = TypedTsv[Double]("output")
  val correct = Map("hello" -> 1, "goodbye" -> 1, "world" -> 2)
}

class SimpleTest(args: Args) extends Job(args) {
  import SimpleTest._

  override def stepStrategy: Option[FlowStepStrategy[JobConf]] = {
    Some(ReducerEstimator)
  }

  def tokenize(text: String): TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")

  val wordCounts = TypedPipe.from(inSrc)
    .flatMap(tokenize)
    .map(_ -> 1)
    .group
    .sum

  val scores = TypedPipe.from(inScores).group

  wordCounts.leftJoin(scores)
    .mapValues{ case (count, score) => (count, score.getOrElse(0.0)) }

    // force another M/R step
    .toTypedPipe
    .map{ case (word, (count, score)) => (count, score) }
    .group.sum

    .toTypedPipe.values.sum
    .write(out)

}

class ReducerEstimatorTest extends Specification {

  import SimpleTest._

  "ReducerEstimator" should {
    val cluster = LocalCluster()
    doFirst { cluster.initialize() }

    "be runnable" in {
      HadoopPlatformJobTest(new SimpleTest(_), cluster)
        .sink[Double](out)(_.head must beCloseTo(2.86, 0.0001))
        .run
    }

    doLast { cluster.shutdown() }
  }

}
