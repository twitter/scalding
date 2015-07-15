package com.twitter.scalding.parquet

import org.apache.parquet.filter2.predicate.FilterPredicate

trait HasFilterPredicate {
  def withFilter: Option[FilterPredicate] = None
}
