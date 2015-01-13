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

object ByteBufferOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[ByteBuffer] => ByteBufferOrderedBuf(c)(tpe)
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._

    def freshT(id: String = "ByteBuffer") = newTermName(c.fresh(s"fresh_$id"))

    def genBinaryCompare = {
      val elementA = freshT("bbA")
      val elementB = freshT("bbB")
      val lenA = freshT("lenA")
      val lenB = freshT("lenB")
      val miniA = freshT("miniA")
      val miniB = freshT("miniB")
      val binaryCompare = q"""
      val $lenA = ${TreeOrderedBuf.injectReadListSize(c)(elementA)}
      val $lenB = ${TreeOrderedBuf.injectReadListSize(c)(elementB)}

      val $miniA = $elementA.slice
      $miniA.limit($lenA)

      val $miniB = $elementB.slice
      $miniB.limit($lenB)

      $miniA.compareTo($miniB)
      """

      (elementA, elementB, binaryCompare)
    }

    val hashVal = freshT("hashVal")
    val hashFn = q"$hashVal.hashCode"

    val getVal = freshT("getVal")
    val lenA = freshT("lenA")
    val miniA = freshT("miniA")

    val getFn = q"""
      val $lenA = ${TreeOrderedBuf.injectReadListSize(c)(getVal)}
      val $miniA = $getVal.slice
      $miniA.limit($lenA)
      $miniA
    """

    def genPutFn = {
      val putBBInput = freshT("putBBInput")
      val putBBdataInput = freshT("putBBdataInput")
      val len = freshT("len")
      val putFn = q"""
      val $len = $putBBdataInput.remaining
      ${TreeOrderedBuf.injectWriteListSize(c)(len, putBBInput)}
      $putBBInput.put($putBBdataInput)
      """
      (putBBInput, putBBdataInput, putFn)
    }

    def genCompareFn = {
      val compareInputA = freshT("compareInputA")
      val compareInputB = freshT("compareInputB")

      val compareFn = q"""
        $compareInputA.compareTo($compareInputB)
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
      override def length(element: Tree): Either[Int, Tree] = {
        val tmpLen = freshT("tmpLen")
        Right(q"""
          val $tmpLen = $element.remaining
          ${TreeOrderedBuf.lengthEncodingSize(c)(q"$tmpLen")} + $tmpLen
        """)
      }
    }
  }
}

