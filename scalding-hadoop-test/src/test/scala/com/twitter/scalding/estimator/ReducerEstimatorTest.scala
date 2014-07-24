package com.twitter.scalding.estimator

import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, LocalCluster }
import org.specs._

object HipJob {
  val inSrc = TextLine(getClass.getResource("/hipster.txt").toString)
  val inScores = TypedTsv[(String, Double)](getClass.getResource("/scores.tsv").toString)
  val out = TypedTsv[Double]("output")
  val correct = Map("hello" -> 1, "goodbye" -> 1, "world" -> 2)
}

class HipJob(args: Args) extends Job(args) {
  import HipJob._

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

  import HipJob._

  "ReducerEstimator" should {
    val cluster = LocalCluster()

    val conf = Config.empty
      .setReducerEstimator(classOf[InputSizeReducerEstimator]) +
      (InputSizeReducerEstimator.BytesPerReducer -> (1L << 14).toString)

    doFirst { cluster.initialize(conf) }

    "run and produce correct output" in {
      HadoopPlatformJobTest(new HipJob(_), cluster)
        .sink[Double](out)(_.head must beCloseTo(2.86, 0.0001))
        .run
    }

    doLast { cluster.shutdown() }
  }

}
