package com.twitter.scalding.examples

import com.twitter.scalding._

/**
 * Example: Count seconds each user spent reading a blog article (using scanLeft)
 * For the sake of simplicity we assume that you have converted date-time into epoch
 */
class ScanLeftTimeExample(args: Args) extends Job(args) {

  // For the most recent event i.e. in the following example for /blog/888 and /blog/999 
  // we cannot assume if user is still reading blog or switched context
  var testList = List(
    (1370737000L, "userA", "/read/blog/123"),
    (1370737002L, "userB", "/read/blog/781"),
    (1370737028L, "userA", "/read/blog/621"),
    (1370737067L, "userB", "/add/comment/"),
    (1370737097L, "userA", "/read/blog/888"),
    (1370737103L, "userB", "/read/blog/999"))

  val orderedPipe =
    IterableSource[(Long, String, String)](testList, ('epoch, 'user, 'event))
      // Create a helper symbol first 
      .insert('temp, 0L)
      // Group by user and sort by epoch in reverse, so that most recent event comes first
      .groupBy('user) { group =>
        group.sortBy('epoch).reverse
          .scanLeft(('epoch, 'temp) -> ('originalEpoch, 'duration))((0L, 0L)) {
            (firstLine: (Long, Long), secondLine: (Long, Long)) =>
              var delta = firstLine._1 - secondLine._1
              // scanLeft is initialised with (0L,0L) so first subtraction 
              // will result into a negative number! 
              if (delta < 0L) delta = -delta
              (secondLine._1, delta)
          }
      }
      // Remove lines introduced by scanLeft and discard helping symbols
      .filter('epoch) {x:Any => x != null}
      .discard('temp, 'originalEpoch)
      // Order in ascending time
      .groupBy('user) { group =>
        group.sortBy('epoch)
      }
      // You can now remove most recent events where we are uncertain of time spent
      .filter('duration) { x:Long => x < 10000L } 
      .write(Tsv("result"))
}
