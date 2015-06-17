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
package com.twitter.scalding.serialization.macros.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.util.Random

import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.macros.impl.ordered_serialization._
import com.twitter.scalding.serialization.macros.impl.ordered_serialization.providers._

object OrderedSerializationProviderImpl {
  def normalizedDispatcher(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if (!tpe.toString.contains(ImplicitOrderedBuf.macroMarker) && !(tpe.normalize == tpe)) =>
      buildDispatcher(tpe.normalize)
  }

  def scaldingBasicDispatchers(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {

    val primitiveDispatcher = PrimitiveOrderedBuf.dispatch(c)
    val optionDispatcher = OptionOrderedBuf.dispatch(c)(buildDispatcher)
    val eitherDispatcher = EitherOrderedBuf.dispatch(c)(buildDispatcher)
    val caseClassDispatcher = CaseClassOrderedBuf.dispatch(c)(buildDispatcher)
    val caseObjectDispatcher = CaseObjectOrderedBuf.dispatch(c)
    val productDispatcher = ProductOrderedBuf.dispatch(c)(buildDispatcher)
    val stringDispatcher = StringOrderedBuf.dispatch(c)
    val traversablesDispatcher = TraversablesOrderedBuf.dispatch(c)(buildDispatcher)
    val unitDispatcher = UnitOrderedBuf.dispatch(c)
    val byteBufferDispatcher = ByteBufferOrderedBuf.dispatch(c)
    val sealedTraitDispatcher = SealedTraitOrderedBuf.dispatch(c)(buildDispatcher)

    OrderedSerializationProviderImpl.normalizedDispatcher(c)(buildDispatcher)
      .orElse(primitiveDispatcher)
      .orElse(unitDispatcher)
      .orElse(optionDispatcher)
      .orElse(eitherDispatcher)
      .orElse(stringDispatcher)
      .orElse(byteBufferDispatcher)
      .orElse(traversablesDispatcher)
      .orElse(caseClassDispatcher)
      .orElse(caseObjectDispatcher)
      .orElse(productDispatcher)
      .orElse(sealedTraitDispatcher)
  }

  def fallbackImplicitDispatcher(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] =
    ImplicitOrderedBuf.dispatch(c)

  private def dispatcher(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._
    def buildDispatcher: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = OrderedSerializationProviderImpl.dispatcher(c)

    scaldingBasicDispatchers(c)(buildDispatcher).orElse(fallbackImplicitDispatcher(c)).orElse {
      case tpe: Type => c.abort(c.enclosingPosition, s"""Unable to find OrderedSerialization for type ${tpe}""")
    }
  }

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[OrderedSerialization[T]] = {
    import c.universe._

    val b: TreeOrderedBuf[c.type] = dispatcher(c)(T.tpe)
    val res = TreeOrderedBuf.toOrderedSerialization[T](c)(b)
    // println(res)
    res
  }
}
