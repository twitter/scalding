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
package com.twitter.scalding.commons.macros.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.util.Random

import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.macros.impl.ordered_serialization._
import com.twitter.scalding.commons.macros.impl.ordered_serialization._
import com.twitter.scalding.macros.impl.OrderedSerializationProviderImpl

object ScroogeInternalOrderedSerializationImpl {
  // The inner dispatcher
  // This one is able to handle all scrooge types along with all normal scala types too
  // One exception is that if it meets another thrift struct it will hit the ScroogeOuterOrderedBuf
  // which will inject an implicit lazy val for a new OrderedSerialization and then exit the macro.
  // This avoids methods becoming too long via inlining.
  private def innerDispatcher(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._
    def buildDispatcher: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = ScroogeInternalOrderedSerializationImpl.innerDispatcher(c)
    val scroogeEnumDispatcher = ScroogeEnumOrderedBuf.dispatch(c)
    val scroogeUnionDispatcher = ScroogeUnionOrderedBuf.dispatch(c)(buildDispatcher)
    val scroogeOuterOrderedBuf = ScroogeOuterOrderedBuf.dispatch(c)

    OrderedSerializationProviderImpl.normalizedDispatcher(c)(buildDispatcher)
      .orElse(scroogeEnumDispatcher)
      .orElse(scroogeUnionDispatcher)
      .orElse(scroogeOuterOrderedBuf)
      .orElse(OrderedSerializationProviderImpl.scaldingBasicDispatchers(c)(buildDispatcher))
      .orElse(OrderedSerializationProviderImpl.fallbackImplicitDispatcher(c))
      .orElse {
        case tpe: Type => c.abort(c.enclosingPosition, s"""Unable to find OrderedSerialization for type ${tpe}""")
      }
  }

  // The outer dispatcher
  // This is the dispatcher routine only hit when we enter in via an external call implicitly or explicitly to the macro.
  // It has the ability to generate code for thrift structs, with the scroogeDispatcher.
  private def outerDispatcher(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._
    def buildOuterDispatcher: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = ScroogeInternalOrderedSerializationImpl.outerDispatcher(c)
    def buildDispatcher: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = ScroogeInternalOrderedSerializationImpl.innerDispatcher(c)

    val innerDisp = innerDispatcher(c)

    val scroogeDispatcher = ScroogeOrderedBuf.dispatch(c)(buildDispatcher)

    OrderedSerializationProviderImpl.normalizedDispatcher(c)(buildOuterDispatcher)
      .orElse(scroogeDispatcher)
      .orElse(innerDisp)
  }

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[OrderedSerialization[T]] = {
    import c.universe._
    val b: TreeOrderedBuf[c.type] = outerDispatcher(c)(T.tpe)
    TreeOrderedBuf.toOrderedSerialization[T](c)(b)
  }
}
