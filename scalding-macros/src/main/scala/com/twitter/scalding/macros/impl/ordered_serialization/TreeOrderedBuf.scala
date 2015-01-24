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
package com.twitter.scalding.macros.impl.ordered_serialization

import scala.reflect.macros.Context
import scala.language.experimental.macros
import java.io.InputStream

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
object CommonCompareBinary {
  import com.twitter.scalding.serialization.JavaStreamEnrichments._

  // If the lengths are equal and greater than this number
  // we will compare on all the containing bytes
  val minSizeForFulBinaryCompare = 24

  final def compareBinaryPrelude(inputStreamA: InputStream,
    lenA: Int,
    inputStreamB: InputStream,
    lenB: Int)(innerCmp: (InputStream, InputStream) => Int) = {
    try {
      // First up validate the lengths passed make sense
      require(lenA >= 0, "Length was " + lenA + "which is < 0, invalid")
      require(lenB >= 0, "Length was " + lenB + "which is < 0, invalid")

      val earlyEqual: Boolean = if (lenA > minSizeForFulBinaryCompare &&
        (lenA == lenB) &&
        inputStreamA.markSupported &&
        inputStreamB.markSupported) {
        inputStreamA.mark(lenA)
        inputStreamB.mark(lenB)

        var pos = 0
        var isSame = true

        while (pos < lenA && isSame == true) {
          inputStreamA.readByte == inputStreamB.readByte
          pos = pos + 1
        }

        if (isSame) {
          isSame
        } else {
          // rewind if they don't match for doing the full compare
          inputStreamA.reset()
          inputStreamA.reset()
          isSame
        }
      } else false

      val r = if (earlyEqual) {
        0
      } else {
        val bufferedStreamA = _root_.com.twitter.scalding.serialization.PositionInputStream(inputStreamA)
        val initialPositionA = bufferedStreamA.position
        val bufferedStreamB = _root_.com.twitter.scalding.serialization.PositionInputStream(inputStreamB)
        val initialPositionB = bufferedStreamB.position

        val innerR = innerCmp(bufferedStreamA, bufferedStreamB)

        bufferedStreamA.seekToPosition(initialPositionA + lenA)
        bufferedStreamB.seekToPosition(initialPositionB + lenB)
        innerR
      }

      OrderedSerialization.resultFrom(r)
    } catch {
      case _root_.scala.util.control.NonFatal(e) =>
        OrderedSerialization.CompareFailure(e)
    }
  }

}
object TreeOrderedBuf {
  import CompileTimeLengthTypes._
  def toOrderedSerialization[T](c: Context)(t: TreeOrderedBuf[c.type])(implicit T: t.ctx.WeakTypeTag[T]): t.ctx.Expr[OrderedSerialization[T]] = {
    import t.ctx.universe._
    def freshT(id: String) = newTermName(c.fresh(s"fresh_$id"))
    val outputLength = freshT("outputLength")

    val innerLengthFn: Tree = {
      val element = freshT("element")

      val tempLen = freshT("tempLen")
      val lensLen = freshT("lensLen")

      val fnBodyOpt = t.length(q"$element") match {
        case _: NoLengthCalculationAvailable[_] => None
        case const: ConstantLengthCalculation[_] => None
        case f: FastLengthCalculation[_] => Some(q"""
        _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(${f.asInstanceOf[FastLengthCalculation[c.type]].t})
        """)
        case m: MaybeLengthCalculation[_] => Some(m.asInstanceOf[MaybeLengthCalculation[c.type]].t)
      }

      fnBodyOpt.map { fnBody =>
        q"""
        private[this] def payloadLength($element: $T): _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.MaybeLength = {
          $fnBody
        }
        """
      }.getOrElse(q"()")
    }

    def binaryLengthGen(typeName: Tree): (Tree, Tree) = {
      val tempLen = freshT("tempLen")
      val lensLen = freshT("lensLen")
      val element = freshT("element")
      val callDynamic = (q"""override def staticSize: Option[Int] = None
""", q"""

      override def dynamicSize($element: $typeName): Option[Int] = {
        val $tempLen = payloadLength($element) match {
          case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation => None
          case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.ConstLen(l) => Some(l)
          case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(l) => Some(l)
        }
        $tempLen.map { case innerLen =>
          val $lensLen = sizeBytes(innerLen)
          innerLen + $lensLen
       }: Option[Int]
     }
      """)
      t.length(q"$element") match {
        case _: NoLengthCalculationAvailable[_] => (q"""
          override def staticSize: Option[Int] = None""", q"""
          override def dynamicSize($element: $typeName): Option[Int] = None
        """)
        case const: ConstantLengthCalculation[_] => (q"""override def staticSize: Option[Int] = Some(${const.toInt})""", q"""
          override def dynamicSize($element: $typeName): Option[Int] = Some(${const.toInt})
          """)
        case f: FastLengthCalculation[_] => callDynamic
        case m: MaybeLengthCalculation[_] => callDynamic
      }
    }

    def putFnGen(outerbaos: TermName, element: TermName) = {
      val tmpArray = freshT("tmpArray")
      val len = freshT("len")
      val oldPos = freshT("oldPos")
      val noLenCalc = q"""

      val $tmpArray = {
        val baos = new _root_.java.io.ByteArrayOutputStream
        innerPutNoLen(baos, $element)
        baos.toByteArray
      }

      val $len = $tmpArray.size
      $outerbaos.writeSize($len)
      $outerbaos.writeBytes($tmpArray)
      """

      def withLenCalc(lenC: Tree) = q"""
        val $len = $lenC
        $outerbaos.writeSize($len)
        innerPutNoLen($outerbaos, $element)
      """

      t.length(q"$element") match {
        case _: NoLengthCalculationAvailable[_] => noLenCalc
        case _: ConstantLengthCalculation[_] => q"""
        innerPutNoLen($outerbaos, $element)
        """
        case f: FastLengthCalculation[_] =>
          withLenCalc(f.asInstanceOf[FastLengthCalculation[c.type]].t)
        case m: MaybeLengthCalculation[_] =>
          val tmpLenRes = freshT("tmpLenRes")
          q"""
            def noLenCalc = {
              $noLenCalc
            }
            def withLenCalc(cnt: Int) = {
              ${withLenCalc(q"cnt")}
            }
            val $tmpLenRes: _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.MaybeLength = payloadLength($element)
            $tmpLenRes match {
              case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation => noLenCalc
              case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.ConstLen(const) => withLenCalc(const)
              case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(s) => withLenCalc(s)
            }
        """
      }
    }

    def readLength(inputStream: TermName) = {
      t.length(q"e") match {
        case const: ConstantLengthCalculation[_] => q"${const.toInt}"
        case _ => q"$inputStream.readSize"
      }
    }

    def discardLength(inputStream: TermName) = {
      t.length(q"e") match {
        case const: ConstantLengthCalculation[_] => q"()"
        case _ => q"$inputStream.readSize"
      }
    }

    val lazyVariables = t.lazyOuterVariables.map {
      case (n, t) =>
        val termName = newTermName(n)
        q"""lazy val $termName = $t"""
    }

    val element = freshT("element")

    val inputStreamA = freshT("inputStreamA")
    val inputStreamB = freshT("inputStreamB")

    val lenA = freshT("lenA")
    val lenB = freshT("lenB")

    t.ctx.Expr[OrderedSerialization[T]](q"""
      new _root_.com.twitter.scalding.serialization.OrderedSerialization[$T] with _root_.com.twitter.bijection.macros.MacroGenerated  {
        import com.twitter.scalding.serialization.JavaStreamEnrichments._
        ..$lazyVariables

        final def innerBinaryCompare($inputStreamA: _root_.java.io.InputStream, $inputStreamB: _root_.java.io.InputStream): Int = {
          ${t.compareBinary(inputStreamA, inputStreamB)}
        }

        override final def compareBinary($inputStreamA: _root_.java.io.InputStream, $inputStreamB: _root_.java.io.InputStream): _root_.com.twitter.scalding.serialization.OrderedSerialization.Result = {

          val $lenA = ${readLength(inputStreamA)}
          val $lenB = ${readLength(inputStreamB)}

          com.twitter.scalding.macros.impl.ordered_serialization.CommonCompareBinary.compareBinaryPrelude($inputStreamA,
            $lenA,
            $inputStreamB,
            $lenB)(innerBinaryCompare)
          }

        def hash(passedInObjectToHash: $T): Int = {
          ${t.hash(newTermName("passedInObjectToHash"))}
        }

        $innerLengthFn

        ${binaryLengthGen(q"$T")._1}
        ${binaryLengthGen(q"$T")._2}


        override def read(from: _root_.java.io.InputStream): _root_.scala.util.Try[$T] = {
          try {
              ${discardLength(newTermName("from"))}
             _root_.scala.util.Success(${t.get(newTermName("from"))})
          } catch { case _root_.scala.util.control.NonFatal(e) =>
            _root_.scala.util.Failure(e)
          }
        }

        private[this] final def innerPutNoLen(into: _root_.java.io.OutputStream, e: $T) {
          ${t.put(newTermName("into"), newTermName("e"))}
        }

        override def write(into: _root_.java.io.OutputStream, e: $T): _root_.scala.util.Try[Unit] = {
          try {
              ${putFnGen(newTermName("into"), newTermName("e"))}
              _root_.com.twitter.scalding.serialization.Serialization.successUnit
          } catch { case _root_.scala.util.control.NonFatal(e) =>
            _root_.scala.util.Failure(e)
          }
        }

        def compare(x: $T, y: $T): Int = {
          ${t.compare(newTermName("x"), newTermName("y"))}
        }
      }
    """)
  }
}

abstract class TreeOrderedBuf[C <: Context] {
  val ctx: C
  val tpe: ctx.Type
  // Expected byte buffers to be in values a and b respestively, the tree has the value of the result
  def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName): ctx.Tree
  // expects the thing to be tested on in the indiciated TermName
  def hash(element: ctx.TermName): ctx.Tree

  // Place input in param 1, tree to return result in param 2
  def get(inputStreamA: ctx.TermName): ctx.Tree

  // BB input in param 1
  // Other input of type T in param 2
  def put(inputStream: ctx.TermName, element: ctx.TermName): ctx.Tree

  def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree

  def lazyOuterVariables: Map[String, ctx.Tree]
  // Return the constant size or a tree
  def length(element: ctx.universe.Tree): CompileTimeLengthTypes[ctx.type]

}
