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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.JavaStreamEnrichments
import java.io.InputStream
import scala.reflect.macros.blackbox.Context
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
   * potentially complex logic in binary comparators
   */
  final def earlyEqual(inputStreamA: InputStream,
    lenA: Int,
    inputStreamB: InputStream,
    lenB: Int): Boolean =
    (lenA > minSizeForFulBinaryCompare &&
      (lenA == lenB) &&
      inputStreamA.markSupported &&
      inputStreamB.markSupported) && {
        inputStreamA.mark(lenA)
        inputStreamB.mark(lenB)

        var pos: Int = 0
        while (pos < lenA) {
          val a = inputStreamA.read
          val b = inputStreamB.read
          pos += 1
          if (a != b) {
            inputStreamA.reset()
            inputStreamB.reset()
            // yeah, return sucks, but trying to optimize here
            return false
          }
          // a == b, but may be eof
          if (a < 0) return JavaStreamEnrichments.eof
        }
        // we consumed all the bytes, and they were all equal
        true
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
        _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(${f.asInstanceOf[FastLengthCalculation[c.type]].t})
        """)
        case m: MaybeLengthCalculation[_] => Some(m.asInstanceOf[MaybeLengthCalculation[c.type]].t)
      }

      fnBodyOpt.map { fnBody =>
        q"""
        private[this] def payloadLength($element: $T): _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MaybeLength = {
          lengthCalculationAttempts += 1
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
        if(skipLenCalc) None else {
          val $tempLen = payloadLength($element) match {
            case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation =>
              failedLengthCalc()
              None
            case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen(l) => Some(l)
            case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(l) => Some(l)
          }
          (if ($tempLen.isDefined) {
            // Avoid a closure here while we are geeking out
            val innerLen = $tempLen.get
            val $lensLen = posVarIntSize(innerLen)
            Some(innerLen + $lensLen)
         } else None): Option[Int]
      }
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

    def genNoLenCalc = {
      val baos = freshT("baos")
      val element = freshT("element")
      val outerOutputStream = freshT("os")
      val len = freshT("len")

      /**
       * This is the worst case: we have to serialize in a side buffer
       * and then see how large it actually is. This happens for cases, like
       * string, where the cost to see the serialized size is not cheaper than
       * directly serializing.
       */
      q"""
      private[this] def noLengthWrite($element: $T, $outerOutputStream: _root_.java.io.OutputStream): Unit = {
        // Start with pretty big buffers because reallocation will be expensive
        val $baos = new _root_.java.io.ByteArrayOutputStream(512)
        ${t.put(baos, element)}
        val $len = $baos.size
        $outerOutputStream.writePosVarInt($len)
        $baos.writeTo($outerOutputStream)
      }
      """
    }

    def putFnGen(outerbaos: TermName, element: TermName) = {
      val oldPos = freshT("oldPos")
      val len = freshT("len")
      /**
       * This is the case where the length is cheap to compute, either
       * constant or easily computable from an instance.
       */
      def withLenCalc(lenC: Tree) = q"""
        val $len = $lenC
        $outerbaos.writePosVarInt($len)
        ${t.put(outerbaos, element)}
      """

      t.length(q"$element") match {
        case _: ConstantLengthCalculation[_] =>
          q"""${t.put(outerbaos, element)}"""
        case f: FastLengthCalculation[_] =>
          withLenCalc(f.asInstanceOf[FastLengthCalculation[c.type]].t)
        case m: MaybeLengthCalculation[_] =>
          val tmpLenRes = freshT("tmpLenRes")
          q"""
            if(skipLenCalc) {
              noLengthWrite($element, $outerbaos)
            } else {
              val $tmpLenRes: _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MaybeLength = payloadLength($element)
              $tmpLenRes match {
                case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation =>
                  failedLengthCalc()
                  noLengthWrite($element, $outerbaos)
                case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen(const) =>
                  ${withLenCalc(q"const")}
                case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(s) =>
                  ${withLenCalc(q"s")}
              }
            }
        """
        case _ => q"noLengthWrite($element, $outerbaos)"
      }
    }

    def readLength(inputStream: TermName) = {
      t.length(q"e") match {
        case const: ConstantLengthCalculation[_] => q"${const.toInt}"
        case _ => q"$inputStream.readPosVarInt"
      }
    }

    def discardLength(inputStream: TermName) = {
      t.length(q"e") match {
        case const: ConstantLengthCalculation[_] => q"()"
        case _ => q"$inputStream.readPosVarInt"
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
    val posStreamA = freshT("posStreamA")
    val posStreamB = freshT("posStreamB")

    val lenA = freshT("lenA")
    val lenB = freshT("lenB")

    t.ctx.Expr[OrderedSerialization[T]](q"""
      new _root_.com.twitter.scalding.serialization.OrderedSerialization[$T] {
        // Ensure macro hygene for Option/Some/None
        import _root_.scala.{Option, Some, None}

        private[this] var lengthCalculationAttempts: Long = 0L
        private[this] var couldNotLenCalc: Long = 0L
        private[this] var skipLenCalc: Boolean = false

        import _root_.com.twitter.scalding.serialization.JavaStreamEnrichments._
        ..$lazyVariables

        override def compareBinary($inputStreamA: _root_.java.io.InputStream, $inputStreamB: _root_.java.io.InputStream): _root_.com.twitter.scalding.serialization.OrderedSerialization.Result =
          try _root_.com.twitter.scalding.serialization.OrderedSerialization.resultFrom {
            val $lenA = ${readLength(inputStreamA)}
            val $lenB = ${readLength(inputStreamB)}
            val $posStreamA = _root_.com.twitter.scalding.serialization.PositionInputStream($inputStreamA)
            val initialPositionA = $posStreamA.position
            val $posStreamB = _root_.com.twitter.scalding.serialization.PositionInputStream($inputStreamB)
            val initialPositionB = $posStreamB.position

            val innerR = ${t.compareBinary(posStreamA, posStreamB)}

            $posStreamA.seekToPosition(initialPositionA + $lenA)
            $posStreamB.seekToPosition(initialPositionB + $lenB)
            innerR
          } catch {
            case _root_.scala.util.control.NonFatal(e) =>
              _root_.com.twitter.scalding.serialization.OrderedSerialization.CompareFailure(e)
          }

        override def hash(passedInObjectToHash: $T): Int = {
          ${t.hash(newTermName("passedInObjectToHash"))}
        }

        private[this] def failedLengthCalc(): Unit = {
          couldNotLenCalc += 1L
          if(lengthCalculationAttempts > 50 && (couldNotLenCalc.toDouble / lengthCalculationAttempts) > 0.4f) {
            skipLenCalc = true
          }
        }

        // What to do if we don't have a length calculation
        $genNoLenCalc

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
