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
package com.twitter.scalding.thrift.macros.impl.ordered_serialization

import com.twitter.scalding.serialization.macros.impl.ordered_serialization._
import com.twitter.scrooge.{ ThriftStruct, ThriftUnion }

import scala.reflect.macros.Context

/*
  ScroogeOuterOrderedBuf is a short cut to stop the macro's recursing onto nested thrift structs.
  An inner one like this puts an outer implicit variable in the current closure.
  The next pass from the compiler will trigger the macro again to build a new class for it.
*/
object ScroogeOuterOrderedBuf {
  // This intentionally handles thrift structs, but not unions, since we want to break out in the struct but not the union
  // That way we can inject all the sub types of the union as implicits into the outer thrift struct.
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if tpe <:< typeOf[ThriftStruct] && !(tpe <:< typeOf[ThriftUnion]) => ScroogeOuterOrderedBuf(c)(tpe)
    }
    pf
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val variableID = (outerType.typeSymbol.fullName.hashCode.toLong + Int.MaxValue.toLong).toString
    val variableNameStr = s"bufferable_$variableID"
    val variableName = newTermName(variableNameStr)
    val implicitInstanciator = q"""_root_.scala.Predef.implicitly[_root_.com.twitter.scalding.serialization.OrderedSerialization[$outerType]]"""

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
            case _root_.scala.Some(s) => _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen(s)
            case _root_.scala.None =>
              $variableName.dynamicSize($element) match {
                case _root_.scala.Some(s) =>
                _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(s)
                case _root_.scala.None =>
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

