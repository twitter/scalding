package com.twitter.scalding.mathematics

import org.specs._
import com.twitter.scalding._

class HistogramJob(args : Args) extends Job(args) {
  try {
  Tsv("input", 'n)
    .groupAll{ _.histogram('n -> 'hist) }
    .flatMapTo('hist -> ('bin, 'cdf)){h : Histogram => h.cdf}
    .write(Tsv("output"))
    } catch {
      case e : Exception => e.printStackTrace()
    }
}

class HistogramJobTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  val inputData = List(1.0, 2.5, 1.5, 3.0, 3.0, 3.0, 4.2, 2.0, 8.0, 1.0).map(Tuple1(_))
  val correctOutput = Set((1.0, 0.3), (2.0, 0.5), (3.0, 0.8), (4.0, 0.9), (8.0, 1.0))
  "A HistogramJob" should {
    JobTest("com.twitter.scalding.mathematics.HistogramJob")
      .source(Tsv("input",('n)), inputData)
      .sink[(Double, Double)](Tsv("output")) { buf =>
        "correctly compute a CDF" in {
          buf.toSet must be_==(correctOutput)
        }
      }
      .run
      .finish
  }
}
