package com.twitter.scalding

import org.specs._
import com.twitter.scalding._

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

/**
 * Advanced example: Count seconds each user spent reading a blog article (using scanLeft)
 * For the sake of simplicity we assume that you have converted date-time into epoch
 */
//class ScanLeftTimeExample(args: Args) extends Job(args) {
//
//  Tsv("input2", ('epoch, 'user, 'event))
//    // Create a helper symbol first 
//    .insert('temp, 0L)
//    // Group by user and sort by epoch in reverse, so that most recent event comes first
//    .groupBy('user) { group =>
//      group.sortBy('epoch).reverse
//        .scanLeft(('epoch, 'temp) -> ('originalEpoch, 'duration))((0L, 0L)) {
//          (firstLine: (Long, Long), secondLine: (Long, Long)) =>
//            var delta = firstLine._1 - secondLine._1
//            // scanLeft is initialised with (0L,0L) so first subtraction 
//            // will result into a negative number! 
//            if (delta < 0L) delta = -delta
//            (secondLine._1, delta)
//        }
//    }
//    .project('epoch, 'user, 'event, 'duration)
//    // Remove lines introduced by scanLeft and discard helping symbols
//    .filter('epoch) { x: Any => x != null }
//    // Order in ascending time
//    .groupBy('user) { group =>
//      group.sortBy('epoch)
//    }
//    // You can now remove most recent events where we are uncertain of time spent
//    .filter('duration) { x: Long => x < 10000L }
//    .debug
//    .write(Tsv("result2"))
//
//}

class ScanLeftTest extends Specification {
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
    JobTest("com.twitter.scalding.AddRankingWithScanLeft")
      .source(Tsv("input1", ('gender, 'height)), sampleInput1)
      .sink[(String, Double, Long)](Tsv("result1")) { outBuf1 =>
        "produce correct number of records when filtering out null values" in {
          outBuf1.size must_== 5
        }
        "create correct ranking per group, 1st being the heighest person of that group" in {
          outBuf1.toSet must_== expectedOutput1
        }
      }
      .run
      .finish
  }

//  // --- A trickier duration counting job
//  var sampleInput2 = List(
//    (1370737000L, "userA", "/read/blog/123"),
//    (1370737002L, "userB", "/read/blog/781"),
//    (1370737028L, "userA", "/read/blog/621"),
//    (1370737067L, "userB", "/add/comment/"),
//    (1370737097L, "userA", "/read/blog/888"),
//    (1370737103L, "userB", "/read/blog/999"))
//
//  // Each group sorted and ranking added highest person to shortest
//  val expectedOutput2 = Set(
//    (1370737000L, "userA", "/read/blog/123", 28), // userA was reading blog/123 for 28 seconds 
//    (1370737028L, "userA", "/read/blog/621", 69), // userA was reading blog/621 for 69 seconds
//    (1370737002L, "userB", "/read/blog/781", 65), // userB was reading blog/781 for 65 seconds
//    (1370737067L, "userB", "/add/comment/", 36)) // userB was posting a comment for 36 seconds
//  // Note that the blog/999 is not recorded as we can't tell how long userB spend on it based on the input
//
//  "A more advanced time extraction scanleft job" should {
//    JobTest("com.twitter.scalding.ScanLeftTimeExample")
//      .source(Tsv("input2", ('epoch, 'user, 'event)), sampleInput2)
//      .sink[(Long, String, String, Long)](Tsv("result2")) { outBuf2 =>
//        "produce correct number of records when filtering out null values" in {
//          outBuf2.size must_== 4
//        }
//        "create correct output per user" in {
//          outBuf2.toSet must_== expectedOutput2
//        }
//      }
//      .run
//      .finish
//  }

}

