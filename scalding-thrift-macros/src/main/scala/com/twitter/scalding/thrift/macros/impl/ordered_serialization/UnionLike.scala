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
package com.twitter.scalding.thrift.macros.impl.ordered_serialization

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.macros.impl.ordered_serialization._

object UnionLike {

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def compareBinary(c: Context)(inputStreamA: c.TermName, inputStreamB: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))
    val valueA = freshT("valueA")
    val valueB = freshT("valueB")
    val idxCmp = freshT("idxCmp")

    val compareSameTypes: Tree = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, optiTBuf)) =>

        val commonCmp: Tree = optiTBuf.map{ tBuf =>
          tBuf.compareBinary(inputStreamA, inputStreamB)
        }.getOrElse[Tree](q"0")

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
                  sys.error("Unable to compare unknown type")
                }""")
        }
    }.get

    q"""
        val $valueA: Int = $inputStreamA.readByte.toInt
        val $valueB: Int = $inputStreamB.readByte.toInt
        val $idxCmp: Int = _root_.java.lang.Integer.compare($valueA, $valueB)
        if($idxCmp != 0) {
          $idxCmp
        } else {
          $compareSameTypes
        }
      """
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def hash(c: Context)(element: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val innerArg = freshT("innerArg")
    subData.foldLeft(Option.empty[Tree]) {
      case (optiExisting, (idx, tpe, optiTBuf)) =>
        val commonPut: Tree = optiTBuf.map { tBuf =>
          q"""{
              val $innerArg: $tpe = $element.asInstanceOf[$tpe]
              ${tBuf.hash(innerArg)}
            }
              """
        }.getOrElse[Tree](q"_root_.scala.Int.MaxValue")

        optiExisting match {
          case Some(s) =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $commonPut ^ _root_.com.twitter.scalding.serialization.Hasher.int.hash($idx)
            } else {
              $s
            }
            """)
          case None =>
            Some(q"""
            if($element.isInstanceOf[$tpe]) {
              $commonPut ^ _root_.com.twitter.scalding.serialization.Hasher.int.hash($idx)
            } else {
              _root_.scala.Int.MaxValue
            }
            """)
        }
    }.get
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def put(c: Context)(inputStream: c.TermName, element: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val innerArg = freshT("innerArg")
    subData.foldLeft(Option.empty[Tree]) {
      case (optiExisting, (idx, tpe, optiTBuf)) =>
        val commonPut: Tree = optiTBuf.map { tBuf =>
          q"""val $innerArg: $tpe = $element.asInstanceOf[$tpe]
              ${tBuf.put(inputStream, innerArg)}
              """
        }.getOrElse[Tree](q"()")

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
  def length(c: Context)(element: c.Tree)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): CompileTimeLengthTypes[c.type] = {
    import CompileTimeLengthTypes._
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val prevSizeData = subData.foldLeft(Option.empty[Tree]) {
      case (optiTree, (idx, tpe, tBufOpt)) =>

        val baseLenT: Tree = tBufOpt.map{ tBuf =>
          tBuf.length(q"$element.asInstanceOf[$tpe]") match {
            case m: MaybeLengthCalculation[_] =>
              m.asInstanceOf[MaybeLengthCalculation[c.type]].t

            case f: FastLengthCalculation[_] =>
              q"""_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(${f.asInstanceOf[FastLengthCalculation[c.type]].t})"""

            case _: NoLengthCalculationAvailable[_] =>
              return NoLengthCalculationAvailable(c)
            case e => sys.error("unexpected input to union length code of " + e)
          }
        }.getOrElse(q"_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(1)")
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
            sys.error("Did not understand thrift union type")
            }""")
        }
    }.get

    MaybeLengthCalculation(c) (prevSizeData)
  }

  // This `_.get` could be removed by switching `subData` to a non-empty list type
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def get(c: Context)(inputStream: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val valueA = freshT("valueA")

    val expandedOut = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, optiTBuf)) =>
        val extract = optiTBuf.map { tBuf =>
          q"""
            ${tBuf.get(inputStream)}
          """
        }.getOrElse {
          q"""(new Object).asInstanceOf[$tpe]"""
        }

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
            sys.error("Did not understand thrift union idx: " + $valueA)
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
  def compare(c: Context)(cmpType: c.Type, elementA: c.TermName, elementB: c.TermName)(subData: List[(Int, c.Type, Option[TreeOrderedBuf[c.type]])]): c.Tree = {
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
                sys.error("Unable to compare unknown type")
              }""")
        }
    }.get

    val compareSameTypes: Option[Tree] = subData.foldLeft(Option.empty[Tree]) {
      case (existing, (idx, tpe, optiTBuf)) =>
        val commonCmp = optiTBuf.map { tBuf =>
          val aTerm = freshT("aTerm")
          val bTerm = freshT("bTerm")
          q"""
          val $aTerm: $tpe = $elementA.asInstanceOf[$tpe]
          val $bTerm: $tpe = $elementB.asInstanceOf[$tpe]
          ${tBuf.compare(aTerm, bTerm)}
        """
        }.getOrElse(q"0")

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

