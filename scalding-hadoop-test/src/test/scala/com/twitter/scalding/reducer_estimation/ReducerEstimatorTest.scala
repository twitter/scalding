package com.twitter.scalding.reducer_estimation

import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopPlatformTest }
import org.scalatest.{ Matchers, WordSpec }
import scala.collection.JavaConverters._

object HipJob {
  val inSrc = TextLine(getClass.getResource("/hipster.txt").toString)
  val inScores = TypedTsv[(String, Double)](getClass.getResource("/scores.tsv").toString)
  val out = TypedTsv[Double]("output")
  val counts = TypedTsv[(String, Int)]("counts.tsv")
  val size = TypedTsv[Long]("size.tsv")
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
    // force another M/R step - should use reducer estimation
    .toTypedPipe
    .map{ case (word, (count, score)) => (count, score) }
    .group.sum
    // force another M/R step - this should force 1 reducer because it is essentially a groupAll
    .toTypedPipe.values.sum
    .write(out)

}

class SimpleJob(args: Args) extends Job(args) {
  import HipJob._
  TypedPipe.from(inSrc)
    .flatMap(_.split("[^\\w]+"))
    .map(_.toLowerCase -> 1)
    .group
    // force the number of reducers to two, to test with/without estimation
    .withReducers(2)
    .sum
    .write(counts)
}

class GroupAllJob(args: Args) extends Job(args) {
  import HipJob._
  TypedPipe.from(inSrc)
    .flatMap(_.split("[^\\w]+"))
    .groupAll
    .size
    .values
    .write(size)
}

class ReducerEstimatorTestSingle extends WordSpec with Matchers with HadoopPlatformTest {
  import HipJob._

  override def initialize() = cluster.initialize(Config.empty
    .addReducerEstimator(classOf[InputSizeReducerEstimator]) +
    (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString))

  "Single-step job with reducer estimator" should {
    "run with correct number of reducers" in {
      HadoopPlatformJobTest(new SimpleJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should contain (2)
        }
        .run
    }
  }
}

class ReducerEstimatorTestSingleOverride extends WordSpec with Matchers with HadoopPlatformTest {
  import HipJob._

  override def initialize() = cluster.initialize(Config.empty
    .addReducerEstimator(classOf[InputSizeReducerEstimator]) +
    (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString) +
    (Config.ReducerEstimatorOverride -> "true"))

  "Single-step job with reducer estimator" should {
    "run with correct number of reducers" in {
      HadoopPlatformJobTest(new SimpleJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should contain (3)
        }
        .run
    }
  }
}

class ReducerEstimatorTestGroupAll extends WordSpec with Matchers with HadoopPlatformTest {
  import HipJob._

  override def initialize() = cluster.initialize(Config.empty
    .addReducerEstimator(classOf[InputSizeReducerEstimator]) +
    (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString))

  "Group-all job with reducer estimator" should {
    "run with correct number of reducers (i.e. 1)" in {
      HadoopPlatformJobTest(new GroupAllJob(_), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should contain (1)
        }
        .run
    }
  }
}

class ReducerEstimatorTestMulti extends WordSpec with Matchers with HadoopPlatformTest {
  import HipJob._

  override def initialize() = cluster.initialize(Config.empty
    .addReducerEstimator(classOf[InputSizeReducerEstimator]) +
    (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString))

  "Multi-step job with reducer estimator" should {
    "run with correct number of reducers in each step" in {
      HadoopPlatformJobTest(new HipJob(_), cluster)
        .sink[Double](out)(_.head shouldBe 2.86 +- 0.0001)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          val reducers = steps.map(_.getConfig.getInt(Config.HadoopNumReducers, 0)).toList
          reducers shouldBe List(3, 1, 1)
        }
        .run
    }
  }
}
