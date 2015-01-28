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
package com.twitter.scalding.macros.impl.ordered_serialization.providers

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.scalding.macros.impl.ordered_serialization.{ CompileTimeLengthTypes, ProductLike, TreeOrderedBuf }
import CompileTimeLengthTypes._
import java.nio.ByteBuffer
import com.twitter.scalding.serialization.OrderedSerialization

object PrimitiveOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[Byte] => PrimitiveOrderedBuf(c)(tpe, "Byte", "readByte", "writeByte", 1)
    case tpe if tpe =:= c.universe.typeOf[Short] => PrimitiveOrderedBuf(c)(tpe, "Short", "readShort", "writeShort", 2)
    case tpe if tpe =:= c.universe.typeOf[Int] => PrimitiveOrderedBuf(c)(tpe, "Integer", "readInt", "writeInt", 4)
    case tpe if tpe =:= c.universe.typeOf[Long] => PrimitiveOrderedBuf(c)(tpe, "Long", "readLong", "writeLong", 8)
    case tpe if tpe =:= c.universe.typeOf[Float] => PrimitiveOrderedBuf(c)(tpe, "Float", "readFloat", "writeFloat", 4)
    case tpe if tpe =:= c.universe.typeOf[Double] => PrimitiveOrderedBuf(c)(tpe, "Double", "readDouble", "writeDouble", 8)
  }

  def apply(c: Context)(outerType: c.Type, javaTypeStr: String, bbGetterStr: String, bbPutterStr: String, lenInBytes: Int): TreeOrderedBuf[c.type] = {
    import c.universe._
    val bbGetter = newTermName(bbGetterStr)
    val bbPutter = newTermName(bbPutterStr)
    val javaType = newTermName(javaTypeStr)

    def freshT(id: String = "Product") = newTermName(c.fresh(s"fresh_$id"))

    def genBinaryCompare(inputStreamA: TermName, inputStreamB: TermName): Tree =
      if (Set("Float", "Double").contains(javaTypeStr)) {
        // These cannot be compared using byte-wise approach
        q"""_root_.java.lang.$javaType.compare($inputStreamA.$bbGetter, $inputStreamB.$bbGetter)"""
      } else {
        // Big endian numbers can be compared byte by byte
        (0 until lenInBytes).map { i =>
          if (i == 0) {
            //signed for the first byte
            q"""_root_.java.lang.Byte.compare($inputStreamA.readByte, $inputStreamB.readByte)"""
          } else {
            q"""_root_.java.lang.Integer.compare($inputStreamA.readUnsignedByte, $inputStreamB.readUnsignedByte)"""
          }
        }
          .toList
          .reverse // go through last to first
          .foldLeft(None: Option[Tree]) {
            case (Some(rest), next) =>
              val nextV = newTermName("next")
              Some(
                q"""val $nextV = $next
                  if ($nextV != 0) $nextV
                  else {
                    $rest
                  }
              """)
            case (None, next) => Some(q"""$next""")
          }.get // there must be at least one item because no primitive is zero bytes
      }

    def genCompareFn(compareInputA: TermName, compareInputB: TermName): Tree = {
      val clamp = Set("Byte", "Short").contains(javaTypeStr)
      val compareFn = if (clamp) {
        val cmpTmpVal = freshT("cmpTmpVal")

        q"""
      val $cmpTmpVal = _root_.java.lang.$javaType.compare($compareInputA, $compareInputB)
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
      _root_.java.lang.$javaType.compare($compareInputA, $compareInputB)
    """
      }
      compareFn
    }

    // used in the hasher
    val typeLowerCase = newTermName(javaTypeStr.toLowerCase)

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = genBinaryCompare(inputStreamA, inputStreamB)
      override def hash(element: ctx.TermName): ctx.Tree = q"_root_.com.twitter.scalding.serialization.Hasher.$typeLowerCase.hash($element)"
      override def put(inputStream: ctx.TermName, element: ctx.TermName) = q"$inputStream.$bbPutter($element)"
      override def get(inputStream: ctx.TermName): ctx.Tree = q"$inputStream.$bbGetter"
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree = genCompareFn(elementA, elementB)
      override def length(element: Tree): CompileTimeLengthTypes[c.type] = ConstantLengthCalculation(c)(lenInBytes)
      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
    }
  }
}

