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
package com.twitter.scalding.macros.impl.ordbufs

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import java.nio.ByteBuffer
import com.twitter.scalding.typed.OrderedBufferable

object PrimitiveOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[Short] => PrimitiveOrderedBuf(c)(tpe, "getShort", "putShort", true, 2)
    case tpe if tpe =:= c.universe.typeOf[Byte] => PrimitiveOrderedBuf(c)(tpe, "get", "put", true, 1)
    case tpe if tpe =:= c.universe.typeOf[Char] => PrimitiveOrderedBuf(c)(tpe, "getChar", "putChar", true, 2)
    case tpe if tpe =:= c.universe.typeOf[Int] => PrimitiveOrderedBuf(c)(tpe, "getInt", "putInt", false, 4)
    case tpe if tpe =:= c.universe.typeOf[Long] => PrimitiveOrderedBuf(c)(tpe, "getLong", "putLong", false, 8)
    case tpe if tpe =:= c.universe.typeOf[Float] => PrimitiveOrderedBuf(c)(tpe, "getFloat", "putFloat", false, 4)
    case tpe if tpe =:= c.universe.typeOf[Double] => PrimitiveOrderedBuf(c)(tpe, "getDouble", "putDouble", false, 8)
  }

  def apply(c: Context)(outerType: c.Type, bbGetterStr: String, bbPutterStr: String, clamp: Boolean, lenInBytes: Int): TreeOrderedBuf[c.type] = {
    val bbGetter = c.universe.newTermName(bbGetterStr)
    val bbPutter = c.universe.newTermName(bbPutterStr)
    import c.universe._

    def freshT(id: String = "Product") = newTermName(c.fresh(s"fresh_$id"))

    def genBinaryCompare = {
      val elementA = freshT("bbA")
      val elementB = freshT("bbB")

      val binaryCompare = if (clamp) {
        val tmpRawVal = freshT("tmpRawVal")
        q"""
      val $tmpRawVal = $elementA.${bbGetter}.compare($elementB.${bbGetter})
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
          $elementA.${bbGetter}.compare($elementB.${bbGetter})
        """
      }
      (elementA, elementB, binaryCompare)
    }

    val hashVal = freshT("hashVal")
    val hashFn = q"$hashVal.hashCode"

    val getVal = freshT("getVal")
    val getFn = q"$getVal.$bbGetter"

    def genPutFn = {
      val putBBInput = freshT("putBBInput")
      val putBBdataInput = freshT("putBBdataInput")
      val putFn = q"$putBBInput.$bbPutter($putBBdataInput)"
      (putBBInput, putBBdataInput, putFn)
    }

    def genCompareFn = {
      val compareInputA = freshT("compareInputA")
      val compareInputB = freshT("compareInputBtB")

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
      (compareInputA, compareInputB, compareFn)
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genBinaryCompare
      override val hash = (hashVal, hashFn)
      override val put = genPutFn
      override val get = (getVal, getFn)
      override val compare = genCompareFn
      override def length(element: Tree): Either[Int, Tree] = Left(lenInBytes)
      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
    }
  }
}

