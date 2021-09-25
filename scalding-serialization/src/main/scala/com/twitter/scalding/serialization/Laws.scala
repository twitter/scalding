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

/**
 * This is a simple trait for describing laws on single parameter
 * type classes (Serialization, Monoid, Ordering, etc...)
 */
sealed trait Law[T] {
  def name: String
}
final case class Law1[T](override val name: String, check: T => Boolean) extends Law[T]
final case class Law2[T](override val name: String, check: (T, T) => Boolean) extends Law[T]
final case class Law3[T](override val name: String, check: (T, T, T) => Boolean) extends Law[T]
