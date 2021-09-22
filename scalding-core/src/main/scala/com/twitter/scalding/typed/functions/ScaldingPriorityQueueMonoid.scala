package com.twitter.scalding.typed.functions

import com.twitter.algebird.mutable.PriorityQueueMonoid

class ScaldingPriorityQueueMonoid[K](
  val count: Int
)(implicit val ordering: Ordering[K]) extends PriorityQueueMonoid[K](count)(ordering)
