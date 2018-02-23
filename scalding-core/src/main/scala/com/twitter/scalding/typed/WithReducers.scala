/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding.typed
import java.io.Serializable

/**
 * used for types that may know how many reducers they need
 * e.g. CoGrouped, Grouped, SortedGrouped, UnsortedGrouped
 */
trait HasReducers {
  def reducers: Option[Int]
}

/**
 * used for types that must know how many reducers they need
 * e.g. Sketched
 */
trait MustHaveReducers extends HasReducers {
  def reducers: Some[Int]
}

/**
 * used for objects that may _set_ how many reducers they need
 * e.g. CoGrouped, Grouped, SortedGrouped, UnsortedGrouped
 */
trait WithReducers[+This <: WithReducers[This]] extends HasReducers {
  /** never mutates this, instead returns a new item. */
  def withReducers(reds: Int): This
}

object WithReducers extends Serializable {
  implicit class Enrichment[W <: WithReducers[W]](val w: W) extends AnyVal {
    def maybeWithReducers(optReducers: Option[Int]): W =
      WithReducers.maybeWithReducers(w, optReducers)
  }

  def maybeWithReducers[W <: WithReducers[W]](w: W, reds: Option[Int]): W =
    reds match {
      case None => w
      case Some(r) => w.withReducers(r)
    }

  /**
   * Return the max of the two number of reducers
   */
  def maybeCombine(optR1: Option[Int], optR2: Option[Int]): Option[Int] =
    (optR1, optR2) match {
      case (None, other) => other
      case (other, None) => other
      case (Some(r1), Some(r2)) => Some(r1 max r2)
    }

}
