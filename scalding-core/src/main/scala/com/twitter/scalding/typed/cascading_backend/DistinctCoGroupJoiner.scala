package com.twitter.scalding.typed.cascading_backend

import cascading.tuple.{ Tuple => CTuple }
import com.twitter.scalding.TupleGetter

// If all the input pipes are unique, this works:
class DistinctCoGroupJoiner[K](count: Int,
  getter: TupleGetter[K],
  @transient joinF: (K, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[Any])
  extends CoGroupedJoiner[K](count, getter, joinF) {
  val distinctSize = count
  def distinctIndexOf(idx: Int) = idx
}
