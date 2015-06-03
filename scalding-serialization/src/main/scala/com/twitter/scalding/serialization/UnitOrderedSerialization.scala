/*
Copyright 2015 Twitter, Inc.

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

package com.twitter.scalding.serialization

import java.io.{ InputStream, OutputStream }

/**
 * Hashes () to 0, writes zero bytes, and everything is equivalent
 */
class UnitOrderedSerialization extends OrderedSerialization[Unit] {
  override def hash(u: Unit) = 0
  override def compare(a: Unit, b: Unit) = 0
  override def read(in: InputStream) = Serialization.successUnit
  override def write(b: OutputStream, u: Unit) = Serialization.successUnit
  override def compareBinary(lhs: InputStream, rhs: InputStream) = OrderedSerialization.resultFrom(0)
  override def staticSize = Some(0)
  override def dynamicSize(u: Unit) = Some(0)
}
