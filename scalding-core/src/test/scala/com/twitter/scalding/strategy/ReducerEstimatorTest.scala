package com.twitter.scalding.strategy;

import cascading.flow.FlowStepStrategy
import com.twitter.scalding._
import org.apache.hadoop.mapred.JobConf
import org.specs._

class TweetCounter(args: Args) extends Job(args) with DefaultDateRangeJob {

  override def stepStrategy: Option[FlowStepStrategy[JobConf]] = {
    println("@> custom StepStrategy!")
    Some(new ReducerEstimator)
  }

  def tokenize(text: String): TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")

  TypedPipe.from(TypedTsv[String]("input"))
    //.filter(_.getText.contains(args("word")))
    //.map(s => 1)
    //.sum

    .flatMap(tokenize)
    .map(_ -> 1)
    .group
    .sum

    .write(TypedTsv("output"))

}

class ReducerEstimatorTest extends Specification {
  "Reducer Estimator Job" should {
    JobTest(new TweetCounter(_))
      .source(TypedTsv[String]("input"),
        List("Hello world", "Goodbye world"))
      .sink[(String, Int)](TypedTsv[(String, Int)]("output")) { outBuf =>
        "ran successfully" in {
          outBuf.toMap must_== Map("hello" -> 1, "goodbye" -> 1, "world" -> 2)
        }
      }
  }
    .runHadoop
    .finish
}
