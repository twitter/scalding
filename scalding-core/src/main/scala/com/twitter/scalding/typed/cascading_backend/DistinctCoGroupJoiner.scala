package com.twitter.scalding.typed.cascading_backend

import com.twitter.scalding.TupleGetter

// If all the input pipes are unique, this works:
class DistinctCoGroupJoiner[K](count: Int,
  getter: TupleGetter[K],
  @transient joinF: (K, Iterator[Any], Seq[Iterable[Any]]) => Iterator[Any])
  extends CoGroupedJoiner[K](count, getter, joinF) {
  val distinctSize = count
  def distinctIndexOf(idx: Int) = idx
}
