package com.twitter.scalding.reducer_estimation

import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopSharedPlatformTest }
import org.scalatest.{ Matchers, WordSpec }
import scala.collection.JavaConverters._

object HipJob {
  val InSrcFileSize = 2496L
  val inSrc = TextLine(getClass.getResource("/hipster.txt").toString) // file size is 2496 bytes
  val InScoresFileSize = 174L
  val inScores = TypedTsv[(String, Double)](getClass.getResource("/scores.tsv").toString) // file size is 174 bytes
  val out = TypedTsv[Double]("output")
  val counts = TypedTsv[(String, Int)]("counts.tsv")
  val size = TypedTsv[Long]("size.tsv")
  val correct = Map("hello" -> 1, "goodbye" -> 1, "world" -> 2)
}

class HipJob(args: Args, customConfig: Config) extends Job(args) {
  import HipJob._
  override def config = super.config ++ customConfig.toMap.toMap

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

class SimpleJob(args: Args, customConfig: Config) extends Job(args) {
  import HipJob._

  override def config = super.config ++ customConfig.toMap.toMap

  TypedPipe.from(inSrc)
    .flatMap(_.split("[^\\w]+"))
    .map(_.toLowerCase -> 1)
    .group
    // force the number of reducers to two, to test with/without estimation
    .withReducers(2)
    .sum
    .write(counts)
}

class GroupAllJob(args: Args, customConfig: Config) extends Job(args) {

  import HipJob._
  override def config = super.config ++ customConfig.toMap.toMap

  TypedPipe.from(inSrc)
    .flatMap(_.split("[^\\w]+"))
    .groupAll
    .size
    .values
    .write(size)
}

class SimpleMapOnlyJob(args: Args, customConfig: Config) extends Job(args) {
  import HipJob._

  override def config = super.config ++ customConfig.toMap.toMap

  // simple job with no reduce phase
  TypedPipe.from(inSrc)
    .flatMap(_.split("[^\\w]+"))
    .write(TypedTsv[String]("mapped_output"))
}

trait ReducerEstimatorTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  import HipJob._

  "Single-step job with reducer estimator" should {
    "run with correct number of reducers" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[InputSizeReducerEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString)

      HadoopPlatformJobTest(new SimpleJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should contain (2)
        }
        .run
    }

    "run with correct number of reducers when overriding set values" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[InputSizeReducerEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString) +
        (Config.ReducerEstimatorOverride -> "true")

      HadoopPlatformJobTest(new SimpleJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should contain (3)
        }
        .run
    }
  }

  "Group-all job with reducer estimator" should {
    "run with correct number of reducers (i.e. 1)" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[InputSizeReducerEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString)

      HadoopPlatformJobTest(new GroupAllJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should contain (1)
        }
        .run
    }
  }

  "Multi-step job with reducer estimator" should {
    "run with correct number of reducers in each step" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[InputSizeReducerEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString)

      HadoopPlatformJobTest(new HipJob(_, customConfig), cluster)
        .sink[Double](out)(_.head shouldBe 2.86 +- 0.0001)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          val reducers = steps.map(step => Config.fromHadoop(step.getConfig).getNumReducers.getOrElse(0)).toList
          reducers shouldBe List(3, 1, 1)
        }
        .run
    }
  }

  "Map-only job with reducer estimator" should {
    "not set num reducers" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[InputSizeReducerEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> (1L << 10).toString)

      HadoopPlatformJobTest(new SimpleMapOnlyJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          val numReducers = conf.getNumReducers
          assert(!numReducers.isDefined || numReducers.get == 0, "Reducers should be 0")
        }
        .run
    }
  }
}

