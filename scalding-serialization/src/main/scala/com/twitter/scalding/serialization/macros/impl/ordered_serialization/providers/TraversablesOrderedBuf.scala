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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization.providers

import scala.language.experimental.macros
import scala.reflect.macros.Context
import java.io.InputStream

import com.twitter.scalding._
import com.twitter.scalding.serialization.macros.impl.ordered_serialization.{ CompileTimeLengthTypes, ProductLike, TreeOrderedBuf }
import CompileTimeLengthTypes._
import com.twitter.scalding.serialization.OrderedSerialization
import scala.reflect.ClassTag

import scala.{ collection => sc }
import scala.collection.{ immutable => sci }

sealed trait ShouldSort
case object DoSort extends ShouldSort
case object NoSort extends ShouldSort

sealed trait MaybeArray
case object IsArray extends MaybeArray
case object NotArray extends MaybeArray

object TraversablesOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.erasure =:= c.universe.typeOf[Iterable[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.Iterable[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[List[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.List[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[Seq[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sc.Seq[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.Seq[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[Vector[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.Vector[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[IndexedSeq[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.IndexedSeq[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.Queue[Any]] => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, NotArray)
    // Arrays are special in that the erasure doesn't do anything
    case tpe if tpe.typeSymbol == c.universe.typeOf[Array[Any]].typeSymbol => TraversablesOrderedBuf(c)(buildDispatcher, tpe, NoSort, IsArray)
    // The erasure of a non-covariant is Set[_], so we need that here for sets
    case tpe if tpe.erasure =:= c.universe.typeOf[Set[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sc.Set[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.Set[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.HashSet[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.ListSet[Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)

    case tpe if tpe.erasure =:= c.universe.typeOf[Map[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sc.Map[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.Map[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.HashMap[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
    case tpe if tpe.erasure =:= c.universe.typeOf[sci.ListMap[Any, Any]].erasure => TraversablesOrderedBuf(c)(buildDispatcher, tpe, DoSort, NotArray)
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
      val (tpe1, tpe2) = (outerType.asInstanceOf[TypeRefApi].args(0), outerType.asInstanceOf[TypeRefApi].args(1)) // linter:ignore
      val containerType = typeOf[Tuple2[Any, Any]].asInstanceOf[TypeRef]
      import compat._
      TypeRef.apply(containerType.pre, containerType.sym, List(tpe1, tpe2))
    } else {
      outerType.asInstanceOf[TypeRefApi].args.head
    }

    val innerTypes = outerType.asInstanceOf[TypeRefApi].args

    val innerBuf: TreeOrderedBuf[c.type] = dispatcher(innerType)
    // TODO it would be nice to capture one instance of this rather
    // than allocate in every call in the materialized class
    val ioa = freshT("ioa")
    val iob = freshT("iob")
    val innerOrd = q"""
      new _root_.scala.math.Ordering[${innerBuf.tpe}] {
        def compare(a: ${innerBuf.tpe}, b: ${innerBuf.tpe}) = {
          val $ioa = a
          val $iob = b
          ${innerBuf.compare(ioa, iob)}
        }
      }
    """

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = {
        val innerCompareFn = freshT("innerCompareFn")
        val a = freshT("a")
        val b = freshT("b")
        q"""
        val $innerCompareFn = { (a: _root_.java.io.InputStream, b: _root_.java.io.InputStream) =>
          val $a = a
          val $b = b
          ${innerBuf.compareBinary(a, b)}
        };
        _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.TraversableHelpers.rawCompare($inputStreamA, $inputStreamB)($innerCompareFn)
      """
      }

      override def put(inputStream: ctx.TermName, element: ctx.TermName) = {
        val asArray = freshT("asArray")
        val bytes = freshT("bytes")
        val len = freshT("len")
        val pos = freshT("pos")
        val innerElement = freshT("innerElement")
        val cmpRes = freshT("cmpRes")

        maybeSort match {
          case DoSort =>
            q"""
          val $len = $element.size
          $inputStream.writePosVarInt($len)

          if($len > 0) {
            val $asArray = $element.toArray[${innerBuf.tpe}]
            // Sorting on the in-memory is the same as binary
            _root_.scala.util.Sorting.quickSort[${innerBuf.tpe}]($asArray)($innerOrd)
            var $pos = 0
            while($pos < $len) {
              val $innerElement = $asArray($pos)
              ${innerBuf.put(inputStream, innerElement)}
              $pos += 1
            }
          }
        """
          case NoSort =>
            q"""
        val $len: Int = $element.size
        $inputStream.writePosVarInt($len)
        $element.foreach { case $innerElement =>
            ${innerBuf.put(inputStream, innerElement)}
        }
        """
        }

      }
      override def hash(element: ctx.TermName): ctx.Tree = {
        val currentHash = freshT("currentHash")
        val len = freshT("len")
        val target = freshT("target")
        maybeSort match {
          case NoSort =>
            q"""
            var $currentHash: Int = _root_.com.twitter.scalding.serialization.MurmurHashUtils.seed
            var $len = 0
            $element.foreach { t =>
              val $target = t
              $currentHash =
                _root_.com.twitter.scalding.serialization.MurmurHashUtils.mixH1($currentHash, ${innerBuf.hash(target)})
              // go ahead and compute the length so we don't traverse twice for lists
              $len += 1
            }
            _root_.com.twitter.scalding.serialization.MurmurHashUtils.fmix($currentHash, $len)
            """
          case DoSort =>
            // We actually don't sort here, which would be expensive, but combine with a commutative operation
            // so the order that we see items won't matter. For this we use XOR
            q"""
            var $currentHash: Int = _root_.com.twitter.scalding.serialization.MurmurHashUtils.seed
            var $len = 0
            $element.foreach { t =>
              val $target = t
              $currentHash = $currentHash ^ ${innerBuf.hash(target)}
              $len += 1
            }
            // Might as well be fancy when we mix in the length
            _root_.com.twitter.scalding.serialization.MurmurHashUtils.fmix($currentHash, $len)
            """
        }
      }

      override def get(inputStream: ctx.TermName): ctx.Tree = {
        val len = freshT("len")
        val firstVal = freshT("firstVal")
        val travBuilder = freshT("travBuilder")
        val iter = freshT("iter")
        val extractionTree = maybeArray match {
          case IsArray =>
            q"""val $travBuilder = new Array[..$innerTypes]($len)
            var $iter = 0
            while($iter < $len) {
              $travBuilder($iter) = ${innerBuf.get(inputStream)}
              $iter = $iter + 1
            }
            $travBuilder : $outerType
            """
          case NotArray =>
            q"""val $travBuilder = $companionSymbol.newBuilder[..$innerTypes]
            $travBuilder.sizeHint($len)
            var $iter = 0
            while($iter < $len) {
              $travBuilder += ${innerBuf.get(inputStream)}
              $iter = $iter + 1
            }
            $travBuilder.result : $outerType
            """
        }
        q"""
        val $len: Int = $inputStream.readPosVarInt
        if($len > 0) {
          if($len == 1) {
            val $firstVal: $innerType = ${innerBuf.get(inputStream)}
            $companionSymbol.apply($firstVal) : $outerType
          } else {
            $extractionTree : $outerType
          }
        } else {
          $companionSymbol.empty : $outerType
        }
      """
      }

      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree = {

        val a = freshT("a")
        val b = freshT("b")
        val cmpFnName = freshT("cmpFnName")
        maybeSort match {
          case DoSort =>
            q"""
              _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.TraversableHelpers.sortedCompare[${innerBuf.tpe}]($elementA, $elementB)($innerOrd)
              """

          case NoSort =>
            q"""
              _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.TraversableHelpers.iteratorCompare[${innerBuf.tpe}]($elementA.iterator, $elementB.iterator)($innerOrd)
              """
        }

      }

      override val lazyOuterVariables: Map[String, ctx.Tree] = innerBuf.lazyOuterVariables

      override def length(element: Tree): CompileTimeLengthTypes[c.type] = {

        innerBuf.length(q"$element.head") match {
          case const: ConstantLengthCalculation[_] =>
            FastLengthCalculation(c)(q"""{
              posVarIntSize($element.size) + $element.size * ${const.toInt}
            }""")
          case m: MaybeLengthCalculation[_] =>
            val maybeRes = freshT("maybeRes")
            MaybeLengthCalculation(c)(q"""
              if($element.isEmpty) {
                val sizeOfZero = 1 // writing the constant 0, for length, takes 1 byte
                _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(sizeOfZero)
              } else {
              val maybeRes = ${m.asInstanceOf[MaybeLengthCalculation[c.type]].t}
              maybeRes match {
                case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen(constSize) =>
                  val sizeOverhead = posVarIntSize($element.size)
                  _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(constSize * $element.size + sizeOverhead)

                  // todo maybe we should support this case
                  // where we can visit every member of the list relatively fast to ask
                  // its length. Should we care about sizes instead maybe?
                case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(_) =>
                   _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation
                case _ => _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation
              }
            }
            """)
          // Something we can't workout the size of ahead of time
          case _ => MaybeLengthCalculation(c)(q"""
              if($element.isEmpty) {
                val sizeOfZero = 1 // writing the constant 0, for length, takes 1 byte
                _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(sizeOfZero)
              } else {
                _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation
              }
            """)
        }
      }
    }
  }
}

