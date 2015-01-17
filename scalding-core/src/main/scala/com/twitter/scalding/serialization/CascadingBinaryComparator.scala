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

import java.io.InputStream
import java.util.Comparator
import cascading.tuple.{ Hasher, StreamComparator }

import scala.util.{ Failure, Success, Try }
/**
 * This is the type that should be fed to cascading to enable binary comparators
 */
class CascadingBinaryComparator[T](ob: OrderedSerialization[T]) extends Comparator[T]
  with StreamComparator[InputStream]
  with Hasher[T]
  with Serializable {

  override def compare(a: T, b: T) = ob.compare(a, b)
  override def hashCode(t: T) = ob.hash(t)
  override def compare(a: InputStream, b: InputStream) =
    ob.compareBinary(a, b).unsafeToInt
}

