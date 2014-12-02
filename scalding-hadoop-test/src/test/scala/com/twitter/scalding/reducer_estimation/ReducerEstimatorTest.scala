package com.twitter.scalding.reducer_estimation

import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, LocalCluster }
import org.apache.hadoop.mapred.JobConf
import org.specs._
import scala.collection.JavaConverters._

object HipJob {
  val inSrc = TextLine(getClass.getResource("/hipster.txt").toString)
  val inScores = TypedTsv[(String, Double)](getClass.getResource("/scores.tsv").toString)
  val out = TypedTsv[Double]("output")
  val countsPath = "counts.tsv"
  val counts = TypedTsv[(String, Int)](countsPath)
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

class SimpleJob(args: Args) extends Job(args) {
  import HipJob._
  TypedPipe.from(inSrc)
    .flatMap(_.split("[^\\w]+"))
    .map(_.toLowerCase -> 1)
    .group
    .sum
    .write(counts)
}

class ReducerEstimatorTest extends Specification {

  import HipJob._

  "Single-step job with reducer estimator" should {

    val cluster = LocalCluster()

    val conf = Config.empty
      .addReducerEstimator(classOf[InputSizeReducerEstimator]) +
      (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString)

    doFirst { cluster.initialize(conf) }

    "run with correct number of reducers" in {
      HadoopPlatformJobTest(new SimpleJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps.size must_== 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers must_== Some(3)
        }
        .run
    }

    doLast { cluster.shutdown() }
  }

  "Multi-step job with reducer estimator" should {
    val cluster = LocalCluster()

    val conf = Config.empty
      .addReducerEstimator(classOf[InputSizeReducerEstimator]) +
      (InputSizeReducerEstimator.BytesPerReducer -> (1L << 16).toString)

    doFirst { cluster.initialize(conf) }

    "run with correct number of reducers in each step" in {
      HadoopPlatformJobTest(new HipJob(_), cluster)
        .sink[Double](out)(_.head must beCloseTo(2.86, 0.0001))
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala

          val reducers = steps.map(_.getConfig.getInt(Config.HadoopNumReducers, 0)).toList
          reducers must_== List(1, 1, 2)
        }
        .run
    }

    doLast { cluster.shutdown() }

  }

}
