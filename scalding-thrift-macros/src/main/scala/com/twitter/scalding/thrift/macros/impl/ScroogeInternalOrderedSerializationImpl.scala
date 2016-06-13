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
package com.twitter.scalding.thrift.macros.impl

import com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl
import com.twitter.scalding.serialization.macros.impl.ordered_serialization._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.thrift.macros.impl.ordered_serialization.{ ScroogeEnumOrderedBuf, ScroogeUnionOrderedBuf, ScroogeOrderedBuf, ScroogeOuterOrderedBuf }

import scala.language.experimental.macros
import scala.reflect.macros.Context

// The flow here is that we start with the outer dispatcher. Outer dispatcher is the only one allowed to recurse into a thrift struct `ScroogeOrderedBuf.dispatch`.
// However it cannot use implicits at the top level. Otherwise this would always be able to return once with an implicit. We will use implicits for members inside
// that struct as needed however.

// The inner ones can recurse into Enum's and Union's, but will use the `ScroogeOuterOrderedBuf` to ensure we drop an implicit down to jump back out.
object ScroogeInternalOrderedSerializationImpl {
  // The base dispatcher
  // This one is able to handle all scrooge types along with all normal scala types too
  // One exception is that if it meets another thrift struct it will hit the ScroogeOuterOrderedBuf
  // which will inject an implicit lazy val for a new OrderedSerialization and then exit the macro.
  // This avoids methods becoming too long via inlining.
  private def baseScroogeDispatcher(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
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
  }

  private def innerDispatcher(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._
    baseScroogeDispatcher(c)
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

    OrderedSerializationProviderImpl.normalizedDispatcher(c)(ScroogeInternalOrderedSerializationImpl.outerDispatcher(c))
      .orElse(ScroogeOrderedBuf.dispatch(c)(baseScroogeDispatcher(c)))
      .orElse(baseScroogeDispatcher(c))
      .orElse {
        case tpe: Type => c.abort(c.enclosingPosition, s"""Unable to find OrderedSerialization for type ${tpe}""")
      }
  }

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[OrderedSerialization[T]] = {
    val b: TreeOrderedBuf[c.type] = outerDispatcher(c)(T.tpe)
    TreeOrderedBuf.toOrderedSerialization[T](c)(b)
  }
}
