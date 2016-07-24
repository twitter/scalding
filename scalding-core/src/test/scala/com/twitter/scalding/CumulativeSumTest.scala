package com.twitter.scalding

import org.scalatest.WordSpec

import com.twitter.scalding.typed.CumulativeSum._

class AddRankingWithCumulativeSum(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[(String, Double)]("input1"))
    .map {
      case (gender, height) =>
        (gender, (height, 1L))
    }
    .cumulativeSum
    .map {
      case (gender, (height, rank)) =>
        (gender, height, rank)
    }
    .write(TypedTsv("result1"))
}

class AddRankingWithPartitionedCumulativeSum(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[(String, Double)]("input1"))
    .map {
      case (gender, height) =>
        (gender, (height, 1L))
    }
    .cumulativeSum { h => (h / 100).floor.toLong }
    .map {
      case (gender, (height, rank)) =>
        (gender, height, rank)
    }
    .write(TypedTsv("result1"))
}

class CumulativeSumTest1 extends WordSpec {

  // --- A simple ranking job
  val sampleInput1 = List(
    ("male", "165.2"),
    ("female", "172.2"),
    ("male", "184.1"),
    ("male", "125.4"),
    ("female", "128.6"),
    ("male", "265.2"),
    ("female", "272.2"),
    ("male", "284.1"),
    ("male", "225.4"),
    ("female", "228.6"))

  // Each group sorted and ranking added highest person to shortest
  val expectedOutput1 = Set(
    ("male", 184.1, 3),
    ("male", 165.2, 2),
    ("male", 125.4, 1),
    ("female", 172.2, 2),
    ("female", 128.6, 1),
    ("male", 284.1, 6),
    ("male", 265.2, 5),
    ("male", 225.4, 4),
    ("female", 272.2, 4),
    ("female", 228.6, 3))

  "A simple ranking cumulative sum job" should {
    JobTest("com.twitter.scalding.AddRankingWithCumulativeSum")
      .source(TypedTsv[(String, Double)]("input1"), sampleInput1)
      .sink[(String, Double, Long)](TypedTsv[(String, Double, Long)]("result1")) { outBuf1 =>
        "produce correct number of records when filtering out null values" in {
          assert(outBuf1.size === 10)
        }
        "create correct ranking per group, 1st being the heighest person of that group" in {
          assert(outBuf1.toSet === expectedOutput1)
        }
      }
      .run
      .finish()
  }

  "A partitioned ranking cumulative sum job" should {
    JobTest("com.twitter.scalding.AddRankingWithPartitionedCumulativeSum")
      .source(TypedTsv[(String, Double)]("input1"), sampleInput1)
      .sink[(String, Double, Long)](TypedTsv[(String, Double, Long)]("result1")) { outBuf1 =>
        "produce correct number of records when filtering out null values" in {
          assert(outBuf1.size === 10)
        }
        "create correct ranking per group, 1st being the heighest person of that group" in {
          assert(outBuf1.toSet === expectedOutput1)
        }
      }
      .run
      .finish()
  }
}
