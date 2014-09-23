package com.twitter.scalding.parquet

import parquet.filter2.predicate.FilterApi._
import parquet.filter2.predicate.FilterPredicate

trait HasFilterPredicate[This <: HasFilterPredicate[This]] {

  def filterPredicate: Option[FilterPredicate] = None

  final def withFilter(fp: FilterPredicate): This = {
    val newFp = filterPredicate match {
      case None => fp
      case Some(old) => and(old, fp)
    }
    copyWithFilter(newFp)
  }

  /**
   * Subclasses must implement this method to return a copy of themselves,
   * but must override filterPredicate to return fp.
   */
  protected def copyWithFilter(fp: FilterPredicate): This

  abstract override def equals(that: Any) =
    super.equals(that) &&
      that.isInstanceOf[HasFilterPredicate[_]] &&
      that.asInstanceOf[HasFilterPredicate[_]].filterPredicate == filterPredicate
}
