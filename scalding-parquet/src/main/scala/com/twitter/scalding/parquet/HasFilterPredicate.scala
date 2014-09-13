package com.twitter.scalding.parquet

import parquet.filter2.predicate.FilterApi._
import parquet.filter2.predicate.FilterPredicate

trait HasFilterPredicate[This <: HasFilterPredicate[This]] {

  def filterPredicate: Option[FilterPredicate] = None

  final def withFilter(fp: FilterPredicate): This = {
    val newFp = filterPredicate.map(prev => and(prev, fp)).getOrElse(fp)
    copyWithFilter(newFp)
  }

  /**
   * Subclasses must implement this method to return a copy of themselves,
   * but must override filterPredicate to return fp.
   */
  protected def copyWithFilter(fp: FilterPredicate): This
}
