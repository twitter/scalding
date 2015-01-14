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
package com.twitter.scalding.commons.macros.impl.ordbufs

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import java.nio.ByteBuffer
import com.twitter.scalding.typed.OrderedBufferable
import com.twitter.scrooge.ThriftEnum
import com.twitter.scalding.macros.impl.ordbufs._

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

    def genBinaryCompare = {
      val elementA = freshT("bbA")
      val elementB = freshT("bbB")

      val valueA = freshT("valueA")
      val valueB = freshT("valueB")

      val binaryCompare = q"""
      val $valueA = ${TreeOrderedBuf.injectReadListSize(c)(elementA)}
      val $valueB = ${TreeOrderedBuf.injectReadListSize(c)(elementB)}

      $valueA.compare($valueB)
      """

      (elementA, elementB, binaryCompare)
    }

    val hashVal = freshT("hashVal")
    val hashFn = q"$hashVal.value.hashCode"

    val getVal = freshT("getVal")
    val valueA = freshT("valueA")

    val getFn = q"""
      val $valueA = ${TreeOrderedBuf.injectReadListSize(c)(getVal)}
      ${outerType.typeSymbol.companionSymbol}.apply($valueA)
    """

    def genPutFn = {
      val putBBInput = freshT("putBBInput")
      val putBBdataInput = freshT("putBBdataInput")
      val putFn = TreeOrderedBuf.injectWriteListSizeTree(c)(q"$putBBdataInput.value", putBBInput)

      (putBBInput, putBBdataInput, putFn)
    }

    def genCompareFn = {
      val compareInputA = freshT("compareInputA")
      val compareInputB = freshT("compareInputBtB")

      val compareFn = q"""
        $compareInputA.value.compare($compareInputB.value)
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
      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty

      override def length(element: Tree): Either[Int, Tree] = Left(4)
    }
  }
}

