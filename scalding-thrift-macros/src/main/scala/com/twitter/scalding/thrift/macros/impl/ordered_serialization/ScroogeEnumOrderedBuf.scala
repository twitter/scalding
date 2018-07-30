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
import com.twitter.scrooge.ThriftEnum

import scala.reflect.macros.Context

object ScroogeEnumOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if tpe <:< typeOf[ThriftEnum] => ScroogeEnumOrderedBuf(c)(tpe)
    }
    pf
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._

    def freshT(id: String) = newTermName(c.fresh(s"fresh_$id"))

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) =
        q"""
        _root_.java.lang.Integer.compare($inputStreamA.readPosVarInt, $inputStreamB.readPosVarInt)
        """

      override def hash(element: ctx.TermName): ctx.Tree =
        q"_root_.com.twitter.scalding.serialization.Hasher.int.hash($element.value)"

      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        q"$inputStream.writePosVarInt($element.value)"
      override def get(inputStream: ctx.TermName): ctx.Tree =
        q"${outerType.typeSymbol.companionSymbol}.apply($inputStream.readPosVarInt)"
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree =
        q"""
        _root_.java.lang.Integer.compare($elementA.value, $elementB.value) : _root_.scala.Int
        """

      override def length(element: Tree): CompileTimeLengthTypes[c.type] = CompileTimeLengthTypes.FastLengthCalculation(c)(q"posVarIntSize($element.value)")
      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
    }
  }
}

