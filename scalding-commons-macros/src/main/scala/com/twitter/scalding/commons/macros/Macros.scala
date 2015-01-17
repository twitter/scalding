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
package com.twitter.scalding.macros

import scala.language.experimental.{ macros => sMacros }

import com.twitter.scalding.commons.thrift.{ ScroogeOrderedBufferable, TBaseOrderedBufferable }
import com.twitter.scrooge.ThriftStruct

import com.twitter.scalding.commons.macros.impl.TBaseOrderedBufferableImpl
import com.twitter.scalding.commons.macros.impl.ScroogeOrderedBufferableImpl

import org.apache.thrift.TBase

object Macros {

  implicit def toTBaseOrderedBufferable[T <: TBase[_, _]]: TBaseOrderedBufferable[T] = macro TBaseOrderedBufferableImpl[T]
  implicit def toScroogeOrderedBufferable[T <: ThriftStruct]: ScroogeOrderedBufferable[T] = macro ScroogeOrderedBufferableImpl[T]

}
