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

object CompareLexographicBB {

  private[this] final val UNSIGNED_MASK: Int = 0xFF

  private[this] final def toInt(value: Byte): Int = value & UNSIGNED_MASK

  def compare(left: ByteBuffer, leftLen: Int,
    right: ByteBuffer,
    rightLen: Int): Int = {
    val minLength = Math.min(leftLen, rightLen)
    var i: Int = 0
    while (i < minLength) {
      val result = toInt(left.get) - toInt(right.get)
      if (result < 0) {
        return -1
      } else if (result > 0) {
        return 1
      }
      i = i + 1
    }
    return leftLen - rightLen
  }
}

object StringOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[String] => StringOrderedBuf(c)(tpe)
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._

    def freshT(id: String = "CaseClassTerm") = newTermName(c.fresh(s"fresh_$id"))

    def bbToSlice(bb: TermName, len: TermName) = {
      val tmpRet = freshT("tmpRet")
      q"""
        val $tmpRet = $bb.slice
        $tmpRet.limit($len)
        $bb.position($bb.position + $len)
        $tmpRet
       """
    }

    def genBinaryCompareFn = {
      val bbA = freshT("bbA")
      val bbB = freshT("bbB")

      val tmpA = freshT("tmpA_BB")
      val tmpB = freshT("tmpB_BB")
      val lenA = freshT("lenA")
      val lenB = freshT("lenB")

      val binaryCompareFn = q"""
        val $lenA = ${TreeOrderedBuf.injectReadListSize(c)(bbA)}
        val $tmpA = ${bbToSlice(bbA, lenA)}
        val $lenB = ${TreeOrderedBuf.injectReadListSize(c)(bbB)}
        val $tmpB = ${bbToSlice(bbB, lenB)}

        _root_.com.twitter.scalding.macros.impl.ordbufs.CompareLexographicBB.compare($tmpA, $lenA, $tmpB, $lenB)

      """
      (bbA, bbB, binaryCompareFn)
    }

    def genHashFn = {
      val hashVal = freshT("hashVal")
      val hashFn = q"$hashVal.hashCode"
      (hashVal, hashFn)
    }

    def genGetFn = {
      val bb = freshT("bb")
      val len = freshT("len")
      val strBytes = freshT("strBytes")
      val getFn = q"""
        val $len = ${TreeOrderedBuf.injectReadListSize(c)(bb)}
        if($len > 0) {
          val $strBytes = new Array[Byte]($len)
          $bb.get($strBytes)
          new String($strBytes, "UTF-8")
          } else {
            ""
          }
      """
      (bb, getFn)
    }

    def genPutFn = {
      val outerBB = freshT("outerStrBB")
      val outerArg = freshT("outerArg")
      val bytes = freshT("bytes")
      val len = freshT("len")
      val outerPutFn = q"""
         val $bytes = $outerArg.getBytes("UTF-8")
         val $len = $bytes.length
         ${TreeOrderedBuf.injectWriteListSize(c)(len, outerBB)}
        if($len > 0) {
          $outerBB.put($bytes)
        }
        """
      (outerBB, outerArg, outerPutFn)
    }

    val compareInputA = freshT("compareInputA")
    val compareInputB = freshT("compareInputB")
    val cmpTmpVal = freshT("cmpTmpVal")
    val compareFn = q"""
      val $cmpTmpVal = $compareInputA.compare($compareInputB)
      if($cmpTmpVal < 0){
        -1
      } else if($cmpTmpVal > 0) {
        1
      } else {
        0
      }
    """

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genBinaryCompareFn
      override val hash = genHashFn
      override val put = genPutFn
      override val get = genGetFn
      override val compare = (compareInputA, compareInputB, compareFn)
      override def length(element: Tree): Either[Int, Tree] = {
        val tmpLen = freshT("tmpLen")
        Right(q"""
          val $tmpLen = $element.getBytes("UTF-8").size
          ${TreeOrderedBuf.lengthEncodingSize(c)(q"$tmpLen")} + $tmpLen
        """)
      }
    }
  }
}

