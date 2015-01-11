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

    def freshT = newTermName(c.fresh(s"freshTerm"))

    def readStrSize(bb: TermName) = {
      val initialB = freshT
      q"""
        val $initialB = $bb.get
        if ($initialB == (-1: Byte)) {
          $bb.getInt
        } else {
          if ($initialB < 0) {
            $initialB.toInt + 256
          } else {
            $initialB.toInt
          }
        }
      """
    }
    def bbToSlice(bb: TermName, len: TermName) = {
      val tmpRet = freshT
      q"""
        val $tmpRet = $bb.slice
        $tmpRet.limit($len)
        $bb.position($bb.position + $len)
        $tmpRet
       """
    }

    def genBinaryCompareFn = {
      val bbA = freshT
      val bbB = freshT

      val tmpA = freshT
      val tmpB = freshT
      val lenA = freshT
      val lenB = freshT
      val binaryCompareFn = q"""
        val $lenA = ${readStrSize(bbA)}
        val $tmpA = ${bbToSlice(bbA, lenA)}
        val $lenB = ${readStrSize(bbB)}
        val $tmpB = ${bbToSlice(bbB, lenB)}

        _root_.com.twitter.scalding.macros.impl.ordbufs.CompareLexographicBB.compare($tmpA, $lenA, $tmpB, $lenB)

      """
      (bbA, bbB, binaryCompareFn)
    }

    def genHashFn = {
      val hashVal = freshT
      val hashFn = q"$hashVal.hashCode"
      (hashVal, hashFn)
    }

    def genGetFn = {
      val bb = freshT
      val len = freshT
      val strBytes = freshT
      val getFn = q"""
        val $len = ${readStrSize(bb)}
        val $strBytes = new Array[Byte]($len)
        $bb.get($strBytes)
        new String($strBytes, "UTF-8")
      """
      (bb, getFn)
    }

    def genPutFn = {
      val outerBB = freshT
      val outerArg = freshT
      val bytes = freshT
      val len = freshT
      val outerPutFn = q"""
         val $bytes = $outerArg.getBytes("UTF-8")
         val $len = $bytes.length
         val startPos = $outerBB.position
         if ($len < 255) {
          $outerBB.put($len.toByte)
         } else {
          $outerBB.put(-1:Byte)
          $outerBB.putInt($len)
        }
        $outerBB.put($bytes)
        val endPos = $outerBB.position
        $outerBB.position(startPos)
        $outerBB.position(endPos)
        """
      (outerBB, outerArg, outerPutFn)
    }

    val compareInputA = freshT
    val compareInputB = freshT
    val cmpTmpVal = freshT
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
    }
  }
}

