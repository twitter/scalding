package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

/**
 * Simple Example: First group data by gender and then sort by height reverse order.
 * Then add another column for each group which is the rank order of the height.
 */
class AddRankingWithScanLeft(args: Args) extends Job(args) {
  Tsv("input1", ('gender, 'height))
    .read
    .groupBy('gender) { group =>
      group.sortBy('height).reverse
      group.scanLeft(('height) -> ('rank))((0L)) {
        (rank: Long, user_id: Double) =>
          {
            (rank + 1L)
          }
      }
    }
    // scanLeft generates an extra line per group, thus remove it
    .filter('height) { x: String => x != null }
    .debug
    .write(Tsv("result1"))
}

class ScanLeftTest extends WordSpec with Matchers {
  import Dsl._

  // --- A simple ranking job
  val sampleInput1 = List(
    ("male", "165.2"),
    ("female", "172.2"),
    ("male", "184.1"),
    ("male", "125.4"),
    ("female", "128.6"))

  // Each group sorted and ranking added highest person to shortest
  val expectedOutput1 = Set(
    ("male", 184.1, 1),
    ("male", 165.2, 2),
    ("male", 125.4, 3),
    ("female", 172.2, 1),
    ("female", 128.6, 2))

  "A simple ranking scanleft job" should {
    JobTest(new AddRankingWithScanLeft(_))
      .source(Tsv("input1", ('gender, 'height)), sampleInput1)
      .sink[(String, Double, Long)](Tsv("result1")) { outBuf1 =>
        "produce correct number of records when filtering out null values" in {
          outBuf1 should have size 5
        }
        "create correct ranking per group, 1st being the heighest person of that group" in {
          outBuf1.toSet shouldBe expectedOutput1
        }
      }
      .run
      .finish()
  }
}
