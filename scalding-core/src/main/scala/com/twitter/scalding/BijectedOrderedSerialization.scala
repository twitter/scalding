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
package com.twitter.scalding

import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.bijection.{ ImplicitBijection, Injection }

object BijectedOrderedSerialization {
  implicit def fromBijection[T, U](implicit bij: ImplicitBijection[T, U], ordSer: OrderedSerialization[U]): OrderedSerialization[T] =
    OrderedSerialization.viaTransform[T, U](bij.apply(_), bij.invert(_))

  implicit def fromInjection[T, U](implicit bij: Injection[T, U], ordSer: OrderedSerialization[U]): OrderedSerialization[T] =
    OrderedSerialization.viaTryTransform[T, U](bij.apply(_), bij.invert(_))
}

