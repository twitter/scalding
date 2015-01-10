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

object TraversablesOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.erasure =:= c.universe.typeOf[List[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe)
    case tpe if tpe.erasure =:= c.universe.typeOf[Seq[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe)
    case tpe if tpe.erasure =:= c.universe.typeOf[Vector[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe)
    // The erasure of a non-covariant is Set[_], so we need that here for sets
    case tpe if tpe.erasure =:= c.universe.typeOf[Set[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe)
    case tpe if tpe.erasure =:= c.universe.typeOf[Map[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe)
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT = newTermName(c.fresh(s"freshTerm"))
    def freshNT(id: String = "TraversableTerm") = newTermName(c.fresh(s"fresh_$id"))

    val dispatcher = buildDispatcher

    val companionSymbol = outerType.typeSymbol.companionSymbol

    val innerType = if (outerType.asInstanceOf[TypeRefApi].args.size == 2) {
      val (tpe1, tpe2) = (outerType.asInstanceOf[TypeRefApi].args(0), outerType.asInstanceOf[TypeRefApi].args(1))
      val containerType = typeOf[Tuple2[Any, Any]].asInstanceOf[TypeRef]
      TypeRef.apply(containerType.pre, containerType.sym, List(tpe1, tpe2))
    } else {
      outerType.asInstanceOf[TypeRefApi].args.head
    }

    val innerTypes = outerType.asInstanceOf[TypeRefApi].args

    val innerBuf: TreeOrderedBuf[c.type] = dispatcher(innerType)

    def readListSize(bb: TermName) = {
      val initialB = freshNT("byteBufferContainer")
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

      val bb = freshNT("bb")
      val len = freshNT("len")
      val firstVal = freshNT("firstVal")
      val travBuilder = freshNT("travBuilder")
      val iter = freshNT("iter")
      val getFn = q"""
        val $len = ${readListSize(bb)}
        val $innerGetVal = $bb
        if($len > 0)
        {
          if($len == 1) {
            val $firstVal = $innerGetFn
            $companionSymbol.apply($firstVal) : $outerType
          } else {
            val $travBuilder = $companionSymbol.newBuilder[..$innerTypes]
            var $iter = 0
            while($iter < $len) {
              $travBuilder += $innerGetFn
              $iter = $iter + 1
            }
            $travBuilder.result : $outerType
          }
        } else {
          $companionSymbol.empty : $outerType
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

