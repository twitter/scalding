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
package com.twitter.scalding.macros.impl.ordser

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import java.nio.ByteBuffer
import com.twitter.scalding.serialization.OrderedSerialization

object PrimitiveOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[Short] => PrimitiveOrderedBuf(c)(tpe, "Short", "readShort", "writeShort", true, 2)
    case tpe if tpe =:= c.universe.typeOf[Byte] => PrimitiveOrderedBuf(c)(tpe, "Byte", "readByte", "writeByte", true, 1)
    case tpe if tpe =:= c.universe.typeOf[Int] => PrimitiveOrderedBuf(c)(tpe, "Integer", "readInt", "writeInt", false, 4)
    case tpe if tpe =:= c.universe.typeOf[Long] => PrimitiveOrderedBuf(c)(tpe, "Long", "readLong", "writeLong", false, 8)
    case tpe if tpe =:= c.universe.typeOf[Float] => PrimitiveOrderedBuf(c)(tpe, "Float", "readFloat", "writeFloat", false, 4)
    case tpe if tpe =:= c.universe.typeOf[Double] => PrimitiveOrderedBuf(c)(tpe, "Double", "readDouble", "writeDouble", false, 8)
  }

  def apply(c: Context)(outerType: c.Type, javaTypeStr: String, bbGetterStr: String, bbPutterStr: String, clamp: Boolean, lenInBytes: Int): TreeOrderedBuf[c.type] = {
    val bbGetter = c.universe.newTermName(bbGetterStr)
    val bbPutter = c.universe.newTermName(bbPutterStr)
    val javaType = c.universe.newTermName(javaTypeStr)
    import c.universe._

    def freshT(id: String = "Product") = newTermName(c.fresh(s"fresh_$id"))

    def genBinaryCompare(inputStreamA: TermName, inputStreamB: TermName): Tree = {
      val binaryCompare = if (clamp) {
        val tmpRawVal = freshT("tmpRawVal")
        q"""
      val $tmpRawVal = _root_.java.lang.$javaType.compare($inputStreamA.${bbGetter}, $inputStreamB.${bbGetter})
      if($tmpRawVal < 0) {
          -1
      } else if($tmpRawVal > 0) {
          1
      } else {
          0
      }
      """
      } else {
        q"""
          _root_.java.lang.$javaType.compare($inputStreamA.${bbGetter}, $inputStreamB.${bbGetter})
        """
      }
      binaryCompare
    }

    def genCompareFn(compareInputA: TermName, compareInputB: TermName): Tree = {

      val compareFn = if (clamp) {
        val cmpTmpVal = freshT("cmpTmpVal")

        q"""
      val $cmpTmpVal = $compareInputA.compare($compareInputB)
      if($cmpTmpVal < 0) {
        -1
      } else if($cmpTmpVal > 0) {
        1
      } else {
        0
      }
    """
      } else {
        q"""
      $compareInputA.compare($compareInputB)
    """
      }
      compareFn
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = genBinaryCompare(inputStreamA, inputStreamB)
      override def hash(element: ctx.TermName): ctx.Tree = q"$element.hashCode"
      override def put(inputStream: ctx.TermName, element: ctx.TermName) = q"$inputStream.$bbPutter($element)"
      override def get(inputStream: ctx.TermName): ctx.Tree = q"$inputStream.$bbGetter"
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree = genCompareFn(elementA, elementB)
      override def length(element: Tree): LengthTypes[c.type] = ConstantLengthCalculation(c)(lenInBytes)
      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
    }
  }
}

