/*
 Copyright 2014 Twitter, Inc.

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

/**
 * There is a Monoid on MaybeLength, with
 * ConstLen(0) being the zero.
 */
sealed trait MaybeLength {
  def +(that: MaybeLength): MaybeLength
}

case object NoLengthCalculation extends MaybeLength {
  def +(that: MaybeLength): MaybeLength = this
}
final case class ConstLen(toInt: Int) extends MaybeLength {
  def +(that: MaybeLength): MaybeLength = that match {
    case ConstLen(c) => ConstLen(toInt + c)
    case DynamicLen(d) => DynamicLen(toInt + d)
    case NoLengthCalculation => NoLengthCalculation
  }
}
final case class DynamicLen(toInt: Int) extends MaybeLength {
  def +(that: MaybeLength): MaybeLength = that match {
    case ConstLen(c) => DynamicLen(toInt + c)
    case DynamicLen(d) => DynamicLen(toInt + d)
    case NoLengthCalculation => NoLengthCalculation
  }
}
