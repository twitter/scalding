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

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.Hasher.int.{ hash => intHash }

object SealedTraitLike {

  /**
   * Compare Binary for generating similar types of binary comparasion code
   * Args:
   * inputStreamA: should contain the variable name that has the input stream A bound to
   * inputStreamB: should contain the variable name that has the input stream B bound to
   * subData: Its a list of the sub components of this sealed trait, for each one
   *          we include an index of this sub type, the clase class/type of this sub type,
   *          and finally a means to compare two instances of this type.
   */
  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def compareBinary(c: Context)(inputStreamA: c.TermName, inputStreamB: c.TermName)(subData: List[(Int, c.Type, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))
    val valueA = freshT("valueA")
    val valueB = freshT("valueB")
    val idxCmp = freshT("idxCmp")

    val compareSameTypes: Tree = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, tBuf)) =>

        val commonCmp: Tree = tBuf.compareBinary(inputStreamA, inputStreamB)

        existing match {
          case Some(t) =>
            Some(q"""
              if($valueA == $idx) {
                $commonCmp
              } else {
                $t
              }
            """)
          case None =>
            Some(q"""
                if($valueA == $idx) {
                  $commonCmp
                } else {
                  sys.error("unreachable code -- this could only be reached by corruption in serialization.")
                }""")
        }
    }.get

    q"""
        val $valueA: Int = $inputStreamA.readByte.toInt
        val $valueB: Int = $inputStreamB.readByte.toInt
        val $idxCmp: Int = _root_.java.lang.Integer.compare($valueA, $valueB)
        if($idxCmp != 0) {
          // Since compare matches, only look at valueA from now on.
          $idxCmp
        } else {
          $compareSameTypes
        }
      """
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def hash(c: Context)(element: c.TermName)(subData: List[(Int, c.Type, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    subData.foldLeft(Option.empty[Tree]) {
      case (optiExisting, (idx, tpe, tBuf)) =>
        val innerArg = freshT("innerArg")
        val elementHash: Tree = q"""
              val $innerArg: $tpe = $element.asInstanceOf[$tpe]
              ${tBuf.hash(innerArg)}
            """

        optiExisting match {
          case Some(s) =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $elementHash ^ ${intHash(idx)}
            } else {
              $s
            }
            """)
          case None =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $elementHash ^ ${intHash(idx)}
            } else {
              _root_.scala.Int.MaxValue
            }
            """)
        }
    }.get
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def put(c: Context)(inputStream: c.TermName, element: c.TermName)(subData: List[(Int, c.Type, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val innerArg = freshT("innerArg")
    subData.foldLeft(Option.empty[Tree]) {
      case (optiExisting, (idx, tpe, tBuf)) =>
        val commonPut: Tree = q"""val $innerArg: $tpe = $element.asInstanceOf[$tpe]
              ${tBuf.put(inputStream, innerArg)}
              """

        optiExisting match {
          case Some(s) =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $inputStream.writeByte($idx.toByte)
              $commonPut
            } else {
              $s
            }
            """)
          case None =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $inputStream.writeByte($idx.toByte)
              $commonPut
            }
            """)
        }
    }.get
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def length(c: Context)(element: c.Tree)(subData: List[(Int, c.Type, TreeOrderedBuf[c.type])]): CompileTimeLengthTypes[c.type] = {
    import CompileTimeLengthTypes._
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val prevSizeData = subData.foldLeft(Option.empty[Tree]) {
      case (optiTree, (idx, tpe, tBuf)) =>

        val baseLenT: Tree = tBuf.length(q"$element.asInstanceOf[$tpe]") match {
          case m: MaybeLengthCalculation[_] =>
            m.asInstanceOf[MaybeLengthCalculation[c.type]].t

          case f: FastLengthCalculation[_] =>
            q"""_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(${f.asInstanceOf[FastLengthCalculation[c.type]].t})"""

          case _: NoLengthCalculationAvailable[_] =>
            return NoLengthCalculationAvailable(c)
          case const: ConstantLengthCalculation[_] =>
            q"""_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen(${const.toInt})"""
          case e => sys.error("unexpected input to union length code of " + e)
        }
        val tmpPreLen = freshT("tmpPreLen")

        val lenT = q"""
        val $tmpPreLen: _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MaybeLength  = $baseLenT

        ($tmpPreLen match {
          case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen(l) =>
            _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(l + 1)
          case _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(l) =>
            _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(l + 1)
          case _ =>
            _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation
          }): _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.MaybeLength
        """
        optiTree match {
          case Some(t) =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $lenT
            } else {
              $t
            }
          """)
          case None =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
            $lenT
          } else {
            sys.error("Unreachable code, did not match sealed trait type")
            }""")
        }
    }.get

    MaybeLengthCalculation(c) (prevSizeData)
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def get(c: Context)(inputStream: c.TermName)(subData: List[(Int, c.Type, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val valueA = freshT("valueA")

    val expandedOut = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, tBuf)) =>
        val extract = q"${tBuf.get(inputStream)}"

        existing match {
          case Some(t) =>
            Some(q"""
            if($valueA == $idx) {
              $extract : $tpe
            } else {
              $t
            }
          """)
          case None =>
            Some(q"""
          if($valueA == $idx) {
            $extract
          } else {
            sys.error("Did not understand sealed trait with idx: " + $valueA + ", this should only happen in a serialization failure.")
          }
            """)
        }
    }.get

    q"""
        val $valueA: Int = $inputStream.readByte.toInt
        $expandedOut
      """
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def compare(c: Context)(cmpType: c.Type, elementA: c.TermName, elementB: c.TermName)(subData: List[(Int, c.Type, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._

    def freshT(id: String) = newTermName(c.fresh(id))

    val arg = freshT("arg")
    val idxCmp = freshT("idxCmp")
    val idxA = freshT("idxA")
    val idxB = freshT("idxB")

    val toIdOpt: Tree = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, _)) =>
        existing match {
          case Some(t) =>
            Some(q"""
            if($arg.isInstanceOf[$tpe]) {
              $idx
            } else {
              $t
            }
          """)
          case None =>
            Some(q"""
              if($arg.isInstanceOf[$tpe]) {
                $idx
              } else {
                sys.error("This should be unreachable code, failure in serializer or deserializer to reach here.")
              }""")
        }
    }.get

    val compareSameTypes: Option[Tree] = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, tBuf)) =>
        val commonCmp = {
          val aTerm = freshT("aTerm")
          val bTerm = freshT("bTerm")
          q"""
          val $aTerm: $tpe = $elementA.asInstanceOf[$tpe]
          val $bTerm: $tpe = $elementB.asInstanceOf[$tpe]
          ${tBuf.compare(aTerm, bTerm)}
        """
        }

        existing match {
          case Some(t) =>
            Some(q"""
            if($idxA == $idx) {
              $commonCmp : Int
            } else {
              $t : Int
            }
          """)
          case None =>
            Some(q"""
              if($idxA == $idx) {
                $commonCmp : Int
              } else {
                $idxCmp
              }""")
        }
    }

    val compareFn = q"""
      def instanceToIdx($arg: $cmpType): Int = {
        ${toIdOpt}: Int
      }

      val $idxA: Int = instanceToIdx($elementA)
      val $idxB: Int = instanceToIdx($elementB)
      val $idxCmp: Int = _root_.java.lang.Integer.compare($idxA, $idxB)

      if($idxCmp != 0) {
        $idxCmp: Int
      } else {
        ${compareSameTypes.get}: Int
      }
    """

    compareFn
  }
}

