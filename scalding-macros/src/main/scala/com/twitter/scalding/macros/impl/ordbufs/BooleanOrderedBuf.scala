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

object BooleanOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[Boolean] => BooleanOrderedBuf(c)(tpe, "get", "put")
  }

  def apply(c: Context)(outerType: c.Type, bbGetterStr: String, bbPutterStr: String): TreeOrderedBuf[c.type] = {
    val bbGetter = c.universe.newTermName(bbGetterStr)
    val bbPutter = c.universe.newTermName(bbPutterStr)
    import c.universe._

    def freshT(id: String = "Product") = newTermName(c.fresh(s"fresh_$id"))

    def genBinaryCompare = {
      val elementA = freshT("bbA")
      val elementB = freshT("bbB")

      val binaryCompare = q"""
      $elementA.${bbGetter}.compare($elementB.${bbGetter})
      """

      (elementA, elementB, binaryCompare)
    }

    val hashVal = freshT("hashVal")
    val hashFn = q"$hashVal.hashCode"

    val getVal = freshT("getVal")
    val getFn = q"""
      if($getVal.$bbGetter == 1) true else false
    """

    def genPutFn = {
      val putBBInput = freshT("putBBInput")
      val putBBdataInput = freshT("putBBdataInput")
      val putFn = q"$putBBInput.$bbPutter(if($putBBdataInput) (1: Byte) else (0: Byte))"
      (putBBInput, putBBdataInput, putFn)
    }

    def genCompareFn = {
      val compareInputA = freshT("compareInputA")
      val compareInputB = freshT("compareInputBtB")

      val compareFn = q"""
        $compareInputA.compare($compareInputB)
      """

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
      override def length(element: Tree): Either[Int, Tree] = Left(1)
      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
    }
  }
}

