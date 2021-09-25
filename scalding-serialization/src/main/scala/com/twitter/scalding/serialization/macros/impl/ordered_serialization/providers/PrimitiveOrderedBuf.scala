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

import scala.reflect.macros.blackbox.Context

import com.twitter.scalding.serialization.macros.impl.ordered_serialization.{
  CompileTimeLengthTypes,
  TreeOrderedBuf
}
import CompileTimeLengthTypes._

object PrimitiveOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[Boolean] =>
      PrimitiveOrderedBuf(c)(tpe, "Boolean", 1, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Boolean] =>
      PrimitiveOrderedBuf(c)(tpe, "Boolean", 1, true)
    case tpe if tpe =:= c.universe.typeOf[Byte] =>
      PrimitiveOrderedBuf(c)(tpe, "Byte", 1, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Byte] =>
      PrimitiveOrderedBuf(c)(tpe, "Byte", 1, true)
    case tpe if tpe =:= c.universe.typeOf[Short] =>
      PrimitiveOrderedBuf(c)(tpe, "Short", 2, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Short] =>
      PrimitiveOrderedBuf(c)(tpe, "Short", 2, true)
    case tpe if tpe =:= c.universe.typeOf[Char] =>
      PrimitiveOrderedBuf(c)(tpe, "Character", 2, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Character] =>
      PrimitiveOrderedBuf(c)(tpe, "Character", 2, true)
    case tpe if tpe =:= c.universe.typeOf[Int] =>
      PrimitiveOrderedBuf(c)(tpe, "Integer", 4, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Integer] =>
      PrimitiveOrderedBuf(c)(tpe, "Integer", 4, true)
    case tpe if tpe =:= c.universe.typeOf[Long] =>
      PrimitiveOrderedBuf(c)(tpe, "Long", 8, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Long] =>
      PrimitiveOrderedBuf(c)(tpe, "Long", 8, true)
    case tpe if tpe =:= c.universe.typeOf[Float] =>
      PrimitiveOrderedBuf(c)(tpe, "Float", 4, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Float] =>
      PrimitiveOrderedBuf(c)(tpe, "Float", 4, true)
    case tpe if tpe =:= c.universe.typeOf[Double] =>
      PrimitiveOrderedBuf(c)(tpe, "Double", 8, false)
    case tpe if tpe =:= c.universe.typeOf[java.lang.Double] =>
      PrimitiveOrderedBuf(c)(tpe, "Double", 8, true)
  }

  def apply(c: Context)(
    outerType: c.Type,
    javaTypeStr: String,
    lenInBytes: Int,
    boxed: Boolean): TreeOrderedBuf[c.type] = {
    import c.universe._
    val javaType = TermName(javaTypeStr)

    def freshT(id: String) = TermName(c.freshName(s"fresh_$id"))

    val shortName: String =
      Map("Integer" -> "Int", "Character" -> "Char").getOrElse(javaTypeStr, javaTypeStr)

    val bbGetter = TermName("read" + shortName)
    val bbPutter = TermName("write" + shortName)

    def genBinaryCompare(inputStreamA: TermName, inputStreamB: TermName): Tree =
      q"""_root_.java.lang.$javaType.compare($inputStreamA.$bbGetter, $inputStreamB.$bbGetter)"""

    def accessor(e: c.TermName): c.Tree = {
      val primitiveAccessor = TermName(shortName.toLowerCase + "Value")
      if (boxed) q"$e.$primitiveAccessor"
      else q"$e"
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) =
        genBinaryCompare(inputStreamA, inputStreamB)
      override def hash(element: ctx.TermName): ctx.Tree = {
        // This calls out the correctly named item in Hasher
        val typeLowerCase = TermName(javaTypeStr.toLowerCase)
        q"_root_.com.twitter.scalding.serialization.Hasher.$typeLowerCase.hash(${accessor(element)})"
      }
      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        q"$inputStream.$bbPutter(${accessor(element)})"

      override def get(inputStream: ctx.TermName): ctx.Tree = {
        val unboxed = q"$inputStream.$bbGetter"
        if (boxed) q"_root_.java.lang.$javaType.valueOf($unboxed)" else unboxed
      }

      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree =
        if (boxed) q"""$elementA.compareTo($elementB)"""
        else q"""_root_.java.lang.$javaType.compare($elementA, $elementB)"""

      override def length(element: Tree): CompileTimeLengthTypes[c.type] =
        ConstantLengthCalculation(c)(lenInBytes)

      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
    }
  }
}
