package com.twitter.scalding.parquet

import parquet.filter2.predicate.FilterPredicate

trait HasFilterPredicate {
  def withFilter: Option[FilterPredicate] = None
}
