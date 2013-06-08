package com.twitter.scalding.examples

import com.twitter.scalding._

/**
 * Example: First group data by gender and then sort by height reverse order.
 * Then add another column for each group which is the rank order of the height.
 */
class AddRankingWithScanLeft(args: Args) extends Job(args) {

  var testList = List(("male", 165.2), ("female", 172.2), ("male", 184.1), ("male", 125.4), ("female", 128.6))
  val orderedPipe =
    IterableSource[(String, Double)](testList, ('gender, 'height))
      .groupBy('gender) { group =>
        group.sortBy('height).reverse
        group.scanLeft(('height) -> ('rank))((0L)) {
          (rank: Long, user_id: Double) =>
            {
              (rank + 1L)
            }
        }
      }
      // scanLeft generates an extra line, thus remove it 
      .filter('height) { x: String => x != null }
      .write(Tsv("result"))
}
