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

object ListOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.erasure =:= c.universe.typeOf[List[Any]] => ListOrderedBuf(c)(buildDispatcher, tpe)
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT = newTermName(c.fresh(s"freshTerm"))
    val dispatcher = buildDispatcher

    val innerType = outerType.asInstanceOf[TypeRefApi].args.head
    val innerBuf: TreeOrderedBuf[c.type] = dispatcher(innerType)

    def readListSize(bb: TermName) = {
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

    def genBinaryCompareFn = {
      val bbA = freshT
      val bbB = freshT

      val tmpA = freshT
      val tmpB = freshT
      val tmpRet = freshT
      val lenA = freshT
      val lenB = freshT
      val minLen = freshT
      val incr = freshT

      val (innerbbA, innerbbB, innerFunc) = innerBuf.compareBinary

      val binaryCompareFn = q"""
        val $lenA = ${readListSize(bbA)}
        val $lenB = ${readListSize(bbB)}

        val $minLen = _root_.scala.math.min($lenA, $lenB)
        var $incr = 0
        val $innerbbA = $bbA
        val $innerbbB = $bbB

        while($incr < $minLen) {
          $innerFunc
          $incr = $incr + 1
        }

        if($lenA < $lenB) {
          return -1
        } else if($lenA > $lenB) {
          return 1
        }
        0

      """
      (bbA, bbB, binaryCompareFn)
    }

    def genHashFn = {
      val hashVal = freshT
      // val (innerHashVal, innerHashFn) = innerBuf.hash
      val hashFn = q"""
        $hashVal.hashCode
      """
      (hashVal, hashFn)
    }

    def genGetFn = {
      val (innerGetVal, innerGetFn) = innerBuf.get

      val bb = freshT
      val len = freshT
      val strBytes = freshT
      val firstVal = freshT
      val listBuffer = freshT
      val iter = freshT
      val getFn = q"""
        val $len = ${readListSize(bb)}
        val $innerGetVal = $bb
        if($len > 0)
        {
          val $firstVal = $innerGetFn
          if($len == 1) {
            List($firstVal)
          } else {
            val $listBuffer = scala.collection.mutable.ListBuffer($firstVal)
            var $iter = 1
            while($iter < $len) {
              $listBuffer += $innerGetFn
              $iter = $iter + 1
            }
            $listBuffer.toList
          }
        } else {
          Nil
        }
      """
      (bb, getFn)
    }

    def genPutFn = {
      val outerBB = freshT
      val outerArg = freshT
      val bytes = freshT
      val len = freshT
      val (innerBB, innerInput, innerPutFn) = innerBuf.put

      val outerPutFn = q"""
        val $innerBB = $outerBB
         val $len = $outerArg.size
         if ($len < 255) {
          $outerBB.put($len.toByte)
         } else {
          $outerBB.put(-1:Byte)
          $outerBB.putInt($len)
        }
        $outerArg.foreach { e =>
          val $innerInput = e
          $innerPutFn
        }
        """
      (outerBB, outerArg, outerPutFn)
    }

    def genCompareFn = {
      val inputA = freshT
      val inputB = freshT
      val (innerInputA, innerInputB, innerCompareFn) = innerBuf.compare

      val lenA = freshT
      val lenB = freshT
      val aIterator = freshT
      val bIterator = freshT
      val minLen = freshT
      val incr = freshT

      val compareFn = q"""
        val $lenA = $inputA.size
        val $lenB = $inputB.size
        val $aIterator = $inputA.toIterator
        val $bIterator = $inputB.toIterator
        val $minLen = _root_.scala.math.min($lenA, $lenB)
        var $incr = 0

        while($incr < $minLen) {
          val $innerInputA = $aIterator.next
          val $innerInputB = $bIterator.next
          $innerCompareFn
          $incr = $incr + 1
        }

        if($lenA < $lenB) {
          return -1
        } else if($lenA > $lenB) {
          return 1
        }
        0
      """

      (inputA, inputB, compareFn)
    }

    val compareInputA = freshT
    val compareInputB = freshT
    val compareFn = q"$compareInputA.compare($compareInputB)"

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genBinaryCompareFn
      override val hash = genHashFn
      override val put = genPutFn
      override val get = genGetFn
      override val compare = genCompareFn
    }
  }
}

