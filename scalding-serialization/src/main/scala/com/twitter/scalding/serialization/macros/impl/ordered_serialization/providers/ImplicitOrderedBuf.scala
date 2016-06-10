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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization.providers

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.macros.impl.ordered_serialization._

/*
  A fall back ordered bufferable to look for the user to have an implicit in scope to satisfy the missing
  type. This is for the case where its an opaque class to our macros where we can't figure out the fields
*/
object ImplicitOrderedBuf {

  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._
    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe => ImplicitOrderedBuf(c)(tpe)
    }
    pf
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val variableID = (outerType.typeSymbol.fullName.hashCode.toLong + Int.MaxValue.toLong).toString
    val variableNameStr = s"orderedSer_$variableID"
    val variableName = newTermName(variableNameStr)

    val implicitInstanciator = q"""
      implicitly[_root_.com.twitter.scalding.serialization.OrderedSerialization[${outerType}]]"""

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) =
        q"$variableName.compareBinary($inputStreamA, $inputStreamB).unsafeToInt"
      override def hash(element: ctx.TermName): ctx.Tree = q"$variableName.hash($element)"

      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        q"$variableName.write($inputStream, $element)"

      override def length(element: Tree) =
        CompileTimeLengthTypes.MaybeLengthCalculation(c)(q"""
          ($variableName.staticSize match {
            case Some(s) => _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen(s)
            case None =>
              $variableName.dynamicSize($element) match {
                case Some(s) =>
                _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(s)
                case None =>
                  _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation
              }
          }): _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MaybeLength
          """)

      override def get(inputStream: ctx.TermName): ctx.Tree =
        q"$variableName.read($inputStream).get"

      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree =
        q"$variableName.compare($elementA, $elementB)"
      override val lazyOuterVariables = Map(variableNameStr -> implicitInstanciator)
    }
  }
}

