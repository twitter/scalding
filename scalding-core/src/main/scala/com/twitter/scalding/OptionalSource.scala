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
package com.twitter.scalding

import scala.util.{ Try, Success, Failure }
import cascading.tap.Tap

case class OptionalSource[T](src: Mappable[T]) extends Source with Mappable[T] {
  override def converter[U >: T] = TupleConverter.asSuperConverter(src.converter)

  def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    Try(src.validateTaps(mode)) match {
      case Success(_) =>
        src.createTap(readOrWrite)
      case Failure(_) =>
        IterableSource[T](Nil)(TupleSetter.singleSetter[T], src.converter)
          .createTap(readOrWrite).asInstanceOf[Tap[_, _, _]]
    }
}