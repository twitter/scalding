/*
 Copyright 2016 Twitter, Inc.

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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers

import com.twitter.scalding.serialization.{ EquivSerialization, OrderedSerialization }

object MacroEqualityOrderedSerialization {
  private val seed = "MacroEqualityOrderedSerialization".hashCode
}

abstract class MacroEqualityOrderedSerialization[T] extends OrderedSerialization[T] with EquivSerialization[T] {
  def uniqueId: String
  override def hashCode = MacroEqualityOrderedSerialization.seed ^ uniqueId.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: MacroEqualityOrderedSerialization[_] => o.uniqueId == uniqueId
    case _ => false
  }
}