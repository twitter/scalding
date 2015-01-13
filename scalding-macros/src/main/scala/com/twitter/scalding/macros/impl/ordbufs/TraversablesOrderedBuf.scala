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

sealed trait ShouldSort
case object DoSort extends ShouldSort
case object NoSort extends ShouldSort

sealed trait MaybeArray
case object IsArray extends MaybeArray
case object NotArray extends MaybeArray

object TraversablesOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.erasure =:= c.universe.typeOf[List[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[Seq[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[Vector[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    // Arrays are special in that the erasure doesn't do anything
    case tpe if tpe.typeSymbol == c.universe.typeOf[Array[Any]].typeSymbol => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, IsArray)
    // The erasure of a non-covariant is Set[_], so we need that here for sets
    case tpe if tpe.erasure =:= c.universe.typeOf[Set[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[scala.collection.Set[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[scala.collection.mutable.Set[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)

    case tpe if tpe.erasure =:= c.universe.typeOf[Map[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[scala.collection.Map[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[scala.collection.mutable.Map[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]],
    outerType: c.Type,
    maybeSort: ShouldSort,
    maybeArray: MaybeArray): TreeOrderedBuf[c.type] = {

    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(s"fresh_$id"))

    val dispatcher = buildDispatcher

    val companionSymbol = outerType.typeSymbol.companionSymbol

    // When dealing with a map we have 2 type args, and need to generate the tuple type
    // it would correspond to if we .toList the Map.
    val innerType = if (outerType.asInstanceOf[TypeRefApi].args.size == 2) {
      val (tpe1, tpe2) = (outerType.asInstanceOf[TypeRefApi].args(0), outerType.asInstanceOf[TypeRefApi].args(1))
      val containerType = typeOf[Tuple2[Any, Any]].asInstanceOf[TypeRef]
      TypeRef.apply(containerType.pre, containerType.sym, List(tpe1, tpe2))
    } else {
      outerType.asInstanceOf[TypeRefApi].args.head
    }

    val innerTypes = outerType.asInstanceOf[TypeRefApi].args

    val innerBuf: TreeOrderedBuf[c.type] = dispatcher(innerType)

    def genBinaryCompareFn = {
      val bbA = freshT("bbA")
      val bbB = freshT("bbB")

      val lenA = freshT("lenA")
      val lenB = freshT("lenB")
      val minLen = freshT("minLen")
      val incr = freshT("incr")

      val (innerbbA, innerbbB, innerFunc) = innerBuf.compareBinary
      val curIncr = freshT("curIncr")

      val binaryCompareFn = q"""
        val $lenA = ${TreeOrderedBuf.injectReadListSize(c)(bbA)}
        val $lenB = ${TreeOrderedBuf.injectReadListSize(c)(bbB)}

        val $minLen = _root_.scala.math.min($lenA, $lenB)
        var $incr = 0
        val $innerbbA = $bbA
        val $innerbbB = $bbB
        var $curIncr = 0
        while($incr < $minLen && $curIncr == 0) {
          $curIncr = $innerFunc
          $incr = $incr + 1
        }

        if($curIncr != 0) {
          $curIncr
        } else {
          if($lenA < $lenB) {
            -1
          } else if($lenA > $lenB) {
            1
          } else {
            0
          }
        }

      """
      (bbA, bbB, binaryCompareFn)
    }

    def genHashFn = {
      val hashVal = freshT("hashVal")
      // val (innerHashVal, innerHashFn) = innerBuf.hash
      val hashFn = q"""
        $hashVal.hashCode
      """
      (hashVal, hashFn)
    }

    def genGetFn = {
      val (innerGetVal, innerGetFn) = innerBuf.get

      val bb = freshT("bb")
      val len = freshT("len")
      val firstVal = freshT("firstVal")
      val travBuilder = freshT("travBuilder")
      val iter = freshT("iter")
      val extractionTree = maybeArray match {
        case IsArray =>
          q"""val $travBuilder = new Array[..$innerTypes]($len)
            var $iter = 0
            while($iter < $len) {
              $travBuilder($iter) = $innerGetFn
              $iter = $iter + 1
            }
            $travBuilder : $outerType
            """
        case NotArray =>
          q"""val $travBuilder = $companionSymbol.newBuilder[..$innerTypes]
            var $iter = 0
            while($iter < $len) {
              $travBuilder += $innerGetFn
              $iter = $iter + 1
            }
            $travBuilder.result : $outerType
            """
      }
      val getFn = q"""
        val $len = ${TreeOrderedBuf.injectReadListSize(c)(bb)}
        val $innerGetVal = $bb
        if($len > 0)
        {
          if($len == 1) {
            val $firstVal = $innerGetFn
            $companionSymbol.apply($firstVal) : $outerType
          } else {
            $extractionTree
          }
        } else {
          $companionSymbol.empty : $outerType
        }
      """
      (bb, getFn)
    }

    def genPutFn = {
      val outerBB = freshT("outerBB")
      val outerArg = freshT("outerArg")
      val bytes = freshT("bytes")
      val len = freshT("len")
      val (innerBB, innerInput, innerPutFn) = innerBuf.put
      val (innerInputA, innerInputB, innerCompareFn) = innerBuf.compare

      val outerPutFn = maybeSort match {
        case DoSort =>

          q"""
          val $len = $outerArg.size
          ${TreeOrderedBuf.injectWriteListSize(c)(len, outerBB)}
          val $innerBB = $outerBB

          if($len > 0) {
          $outerArg.toArray.sortWith { (a, b) =>
              val $innerInputA = a
              val $innerInputB = b
              val cmpRes = $innerCompareFn
              cmpRes < 0
          }.foreach{ e =>
            val $innerInput = e
            $innerPutFn
            }
          }
        """
        case NoSort =>
          q"""
        val $len = $outerArg.size
        ${TreeOrderedBuf.injectWriteListSize(c)(len, outerBB)}
        val $innerBB = $outerBB
        $outerArg.foreach { e =>
          val $innerInput = e
          $innerPutFn
        }
        """
      }

      (outerBB, outerArg, outerPutFn)
    }

    def genCompareFn = {
      val inputA = freshT("inputA")
      val inputB = freshT("inputB")
      val (innerInputA, innerInputB, innerCompareFn) = innerBuf.compare

      val lenA = freshT("lenA")
      val lenB = freshT("lenB")
      val aIterator = freshT("aIterator")
      val bIterator = freshT("bIterator")
      val minLen = freshT("minLen")
      val incr = freshT("incr")
      val curIncr = freshT("curIncr")
      val (iterA, iterB) = maybeSort match {
        case DoSort =>
          (q"""
        val $aIterator: Iterator[$innerType] = $inputA.toArray.sortWith { (a: $innerType, b: $innerType) =>
            val $innerInputA: $innerType = a
            val $innerInputB: $innerType = b
            val cmpRes = $innerCompareFn
            cmpRes < 0
          }.toIterator""", q"""
        val $bIterator: Iterator[$innerType] = $inputB.toArray.sortWith { (a: $innerType, b: $innerType) =>
            val $innerInputA: $innerType = a
            val $innerInputB: $innerType = b
            val cmpRes = $innerCompareFn
            cmpRes < 0
          }.toIterator""")
        case NoSort =>
          (q"""
          val $aIterator = $inputA.toIterator
          """, q"""
          val $bIterator = $inputB.toIterator
          """)
      }

      val compareFn = q"""
        val $lenA: Int = $inputA.size
        val $lenB: Int = $inputB.size
        $iterA
        $iterB
        val $minLen: Int = _root_.scala.math.min($lenA, $lenB)
        var $incr: Int = 0
        var $curIncr: Int = 0
        while($incr < $minLen && $curIncr == 0 ) {
          val $innerInputA: $innerType = $aIterator.next
          val $innerInputB: $innerType = $bIterator.next
          $curIncr = $innerCompareFn
          $incr = $incr + 1
        }

        if($curIncr != 0) {
          $curIncr
        } else {
          if($lenA < $lenB) {
            -1
          } else if($lenA > $lenB) {
            1
          } else {
            0
          }
        }
      """

      (inputA, inputB, compareFn)
    }

    val compareInputA = freshT("compareInputA")
    val compareInputB = freshT("compareInputB")
    val compareFn = q"$compareInputA.compare($compareInputB)"

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genBinaryCompareFn
      override val hash = genHashFn
      override val put = genPutFn
      override val get = genGetFn
      override val compare = genCompareFn
      override def length(element: Tree): Either[Int, Tree] = {
        innerBuf.length(q"$element.a") match {
          case Left(s) =>
            Right(q"""{
              ${TreeOrderedBuf.lengthEncodingSize(c)(q"$element.size")} + $element.size * $s
            }
              """)
          case Right(t) =>
            Right(q"""{
          ${TreeOrderedBuf.lengthEncodingSize(c)(q"$element.size")} + $element.foldLeft(0){ case (cur, next) =>
            cur + ${
              innerBuf.length(q"next").right.get
            }
          }
          }""")
        }
      }
    }
  }
}

