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

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.PositionInputStream
import java.io.InputStream
import scala.reflect.macros.Context
import scala.language.experimental.macros
import scala.util.control.NonFatal

object CommonCompareBinary {
  import com.twitter.scalding.serialization.JavaStreamEnrichments._

  // If the lengths are equal and greater than this number
  // we will compare on all the containing bytes
  val minSizeForFulBinaryCompare = 24

  /**
   * This method will compare two InputStreams of given lengths
   * If the inputsteam supports mark/reset (such as those backed by Array[Byte]),
   * and the lengths are equal and longer than minSizeForFulBinaryCompare we first
   * check if they are byte-for-byte identical, which is a cheap way to avoid doing
   * potentially complex logic in innerCmp
   *
   * If the above fails to show them equal, we apply innerCmp. Note innerCmp does
   * not need to seek each stream to the end of the records, that is handled by
   * this method after innerCmp returns
   */
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

        @annotation.tailrec
        def arrayBytesSame(pos: Int): Boolean =
          (pos >= lenA) ||
            ((inputStreamA.readByte == inputStreamB.readByte) &&
              arrayBytesSame(pos + 1))

        arrayBytesSame(0) || {
          // rewind if they don't match for doing the full compare
          inputStreamA.reset()
          inputStreamB.reset()
          false
        }
      } else false

      val r = if (earlyEqual) {
        0
      } else {
        val bufferedStreamA = PositionInputStream(inputStreamA)
        val initialPositionA = bufferedStreamA.position
        val bufferedStreamB = PositionInputStream(inputStreamB)
        val initialPositionB = bufferedStreamB.position

        val innerR = innerCmp(bufferedStreamA, bufferedStreamB)

        bufferedStreamA.seekToPosition(initialPositionA + lenA)
        bufferedStreamB.seekToPosition(initialPositionB + lenB)
        innerR
      }

      OrderedSerialization.resultFrom(r)
    } catch {
      case NonFatal(e) => OrderedSerialization.CompareFailure(e)
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
        @inline private[this] def payloadLength($element: $T): _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.MaybeLength = {
          $fnBody
        }
        """
      }.getOrElse(q"()")
    }

    def binaryLengthGen(typeName: Tree): (Tree, Tree) = {
      val tempLen = freshT("tempLen")
      val lensLen = freshT("lensLen")
      val element = freshT("element")
      val callDynamic = (q"""override def staticSize: Option[Int] = None""",
        q"""

      override def dynamicSize($element: $typeName): Option[Int] = {
        val $tempLen = payloadLength($element) match {
          case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation => None
          case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.ConstLen(l) => Some(l)
          case _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(l) => Some(l)
        }
        (if ($tempLen.isDefined) {
          // Avoid a closure here while we are geeking out
          val innerLen = $tempLen.get
          val $lensLen = sizeBytes(innerLen)
          Some(innerLen + $lensLen)
       } else None): Option[Int]
     }
      """)
      t.length(q"$element") match {
        case _: NoLengthCalculationAvailable[_] => (q"""
          override def staticSize: Option[Int] = None""", q"""
          override def dynamicSize($element: $typeName): Option[Int] = None""")
        case const: ConstantLengthCalculation[_] => (q"""
          override val staticSize: Option[Int] = Some(${const.toInt})""", q"""
          override def dynamicSize($element: $typeName): Option[Int] = staticSize""")
        case f: FastLengthCalculation[_] => callDynamic
        case m: MaybeLengthCalculation[_] => callDynamic
      }
    }

    def putFnGen(outerbaos: TermName, element: TermName) = {
      val baos = freshT("baos")
      val len = freshT("len")
      val oldPos = freshT("oldPos")

      /**
       * This is the worst case: we have to serialize in a side buffer
       * and then see how large it actually is. This happens for cases, like
       * string, where the cost to see the serialized size is not cheaper than
       * directly serializing.
       */
      val noLenCalc = q"""
      val $baos = new _root_.java.io.ByteArrayOutputStream
      ${t.put(baos, element)}
      val $len = $baos.size
      $outerbaos.writeSize($len)
      $baos.writeTo($outerbaos)
      """

      /**
       * This is the case where the length is cheap to compute, either
       * constant or easily computable from an instance.
       */
      def withLenCalc(lenC: Tree) = q"""
        val $len = $lenC
        $outerbaos.writeSize($len)
        ${t.put(outerbaos, element)}
      """

      t.length(q"$element") match {
        case _: NoLengthCalculationAvailable[_] => noLenCalc
        case _: ConstantLengthCalculation[_] =>
          q"""${t.put(outerbaos, element)}"""
        case f: FastLengthCalculation[_] =>
          withLenCalc(f.asInstanceOf[FastLengthCalculation[c.type]].t)
        case m: MaybeLengthCalculation[_] =>
          val tmpLenRes = freshT("tmpLenRes")
          q"""
            @inline def noLenCalc = {
              $noLenCalc
            }
            @inline def withLenCalc(cnt: Int) = {
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
        import _root_.com.twitter.scalding.serialization.JavaStreamEnrichments._
        ..$lazyVariables

        private[this] val innerBinaryCompare = { ($inputStreamA: _root_.java.io.InputStream, $inputStreamB: _root_.java.io.InputStream) =>
          ${t.compareBinary(inputStreamA, inputStreamB)}
        }

        override def compareBinary($inputStreamA: _root_.java.io.InputStream, $inputStreamB: _root_.java.io.InputStream): _root_.com.twitter.scalding.serialization.OrderedSerialization.Result = {

          val $lenA = ${readLength(inputStreamA)}
          val $lenB = ${readLength(inputStreamB)}

          com.twitter.scalding.macros.impl.ordered_serialization.CommonCompareBinary.compareBinaryPrelude($inputStreamA,
            $lenA,
            $inputStreamB,
            $lenB)(innerBinaryCompare)
          }

        override def hash(passedInObjectToHash: $T): Int = {
          ${t.hash(newTermName("passedInObjectToHash"))}
        }

        // defines payloadLength private method
        $innerLengthFn

        // static size:
        ${binaryLengthGen(q"$T")._1}

        // dynamic size:
        ${binaryLengthGen(q"$T")._2}

        override def read(from: _root_.java.io.InputStream): _root_.scala.util.Try[$T] = {
          try {
              ${discardLength(newTermName("from"))}
             _root_.scala.util.Success(${t.get(newTermName("from"))})
          } catch { case _root_.scala.util.control.NonFatal(e) =>
            _root_.scala.util.Failure(e)
          }
        }

        override def write(into: _root_.java.io.OutputStream, e: $T): _root_.scala.util.Try[Unit] = {
          try {
              ${putFnGen(newTermName("into"), newTermName("e"))}
              _root_.com.twitter.scalding.serialization.Serialization.successUnit
          } catch { case _root_.scala.util.control.NonFatal(e) =>
            _root_.scala.util.Failure(e)
          }
        }

        override def compare(x: $T, y: $T): Int = {
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
