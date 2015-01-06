/*
Copyright 2013 Twitter, Inc.

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
package com.twitter.scalding.commons

import scala.language.experimental.{ macros => sMacros }

import com.twitter.scalding.commons.thrift.TBaseOrderedBufferable
import com.twitter.scalding.commons.macros.impl.TBaseOrderedBufferableImpl
import org.apache.thrift.TBase

// The implicits in the jdbc.macro's package
// These are to allow us to auto provide our Type Classes without the user possibly knowing
// all of the various ways we could build it.
package object macros {
  implicit def toTBaseOrderedBufferable[T <: TBase[_, _]]: TBaseOrderedBufferable[T] = macro TBaseOrderedBufferableImpl[T]

}
