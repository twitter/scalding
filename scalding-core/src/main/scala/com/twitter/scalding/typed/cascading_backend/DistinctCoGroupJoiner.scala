package com.twitter.scalding.typed.cascading_backend

import com.twitter.scalding.TupleGetter
import com.twitter.scalding.typed.MultiJoinFunction

// If all the input pipes are unique, this works:
class DistinctCoGroupJoiner[K](count: Int,
  getter: TupleGetter[K],
  @transient joinF: MultiJoinFunction[K, Any])
  extends CoGroupedJoiner[K](count, getter, joinF) {
  val distinctSize = count
  def distinctIndexOf(idx: Int) = idx
}
