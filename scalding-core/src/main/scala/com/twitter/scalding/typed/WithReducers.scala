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

/**
 * used for types that may know how many reducers they need
 * e.g. CoGrouped, Grouped, SortedGrouped, UnsortedGrouped
 */
trait HasReducers {
  def reducers: Option[Int]
}

/**
 * used for objects that may _set_ how many reducers they need
 * e.g. CoGrouped, Grouped, SortedGrouped, UnsortedGrouped
 */
trait WithReducers[+This <: WithReducers[This]] extends HasReducers {
  /** never mutates this, instead returns a new item. */
  def withReducers(reds: Int): This
}
