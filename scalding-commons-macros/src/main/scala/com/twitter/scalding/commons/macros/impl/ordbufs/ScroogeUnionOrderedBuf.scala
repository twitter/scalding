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
package com.twitter.scalding.commons.macros.impl.ordbufs

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import java.nio.ByteBuffer
import com.twitter.scalding.typed.OrderedBufferable
import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scrooge.ThriftUnion
import com.twitter.scalding.macros.impl.ordbufs._

object ScroogeUnionOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if tpe <:< typeOf[ThriftUnion] &&
        (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isTrait) &&
        !tpe.typeSymbol.asClass.knownDirectSubclasses.isEmpty => ScroogeUnionOrderedBuf(c)(buildDispatcher, tpe)
    }
    pf
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(s"$id"))

    val dispatcher = buildDispatcher

    val subClasses: List[Type] = outerType.typeSymbol.asClass.knownDirectSubclasses.map(_.asType.toType).toList

    val subData: List[(Int, Type, Option[TreeOrderedBuf[c.type]])] = subClasses.map { t =>
      if (t.typeSymbol.name.toString == "UnknownUnionField") {
        (t, None)
      } else {
        (t, Some(dispatcher(t)))
      }
    }.zipWithIndex.map{ case ((tpe, tbuf), idx) => (idx, tpe, tbuf) }.toList

    require(subData.size > 0, "Must have some sub types on a union?")

    def genHashFn = {
      val hashVal = freshT("hashVal")
      val hashFn = q"$hashVal.hashCode"
      (hashVal, hashFn)
    }

    def genBinaryCompare = {
      val bbA = freshT("bbAAA")
      val bbB = freshT("bbBBB")
      val valueA = freshT("valueA")
      val valueB = freshT("valueB")
      val idxCmp = freshT("idxCmp")

      val compareSameTypes: Option[Tree] = subData.foldLeft(Option.empty[Tree]) {
        case (existing, (idx, tpe, optiTBuf)) =>

          val commonCmp = optiTBuf.map{ tBuf =>
            val (aTerm, bTerm, cmp) = tBuf.compareBinary
            q"""
              val $aTerm: _root_.java.nio.ByteBuffer = $bbA
              val $bTerm: _root_.java.nio.ByteBuffer = $bbB
              $cmp
            """
          }.getOrElse(q"0")

          existing match {
            case Some(t) =>
              Some(q"""
              if($idxCmp == $idx) {
                $commonCmp
              } else {
                $t
              }
            """)
            case None =>
              Some(q"""
                if($idxCmp == $idx) {
                  $commonCmp
                } else {
                  sys.error("Unable to compare unknown type")
                }""")
          }
      }

      val binaryCmpTree = q"""
        val $valueA: Int = $bbA.get.toInt
        val $valueB: Int = $bbB.get.toInt
        val $idxCmp: Int = $valueA.compare($valueB)
        if($idxCmp != 0) {
          $idxCmp
        } else {
          ${compareSameTypes.get}
        }
      """

      (bbA, bbB, binaryCmpTree)
    }

    def genPutFn = {
      val putBBInput = freshT("putBBInput")
      val putBBdataInput = freshT("putBBdataInput")

      val expandedOut: Tree = subData.foldLeft(Option.empty[Tree]) {
        case (optiExisting, (idx, tpe, optiTBuf)) =>
          val commonPut: Tree = optiTBuf.map{ tBuf =>
            val (innerBB, innerArg, innerPutFn) = tBuf.put
            q"""val $innerBB: _root_.java.nio.ByteBuffer = $putBBInput
                val $innerArg: $tpe = $putBBdataInput.asInstanceOf[$tpe]
                $innerPutFn
                """
          }.getOrElse(q"()")

          optiExisting match {
            case Some(s) =>
              Some(q"""
              if($putBBdataInput.isInstanceOf[$tpe]) {
                $putBBInput.put($idx.toByte)
                $commonPut
              } else {
                $s
              }
              """)
            case None =>
              Some(q"""
              if($putBBdataInput.isInstanceOf[$tpe]) {
                $putBBInput.put($idx.toByte)
                $commonPut
              }
              """)
          }
      }.get

      (putBBInput, putBBdataInput, expandedOut)
    }

    def genLength(element: Tree): Either[Int, Tree] = {
      val expandedOut: Option[Tree] = subData.foldLeft(Option.empty[Tree]) {
        case (existing, (idx, tpe, tBufOpt)) =>
          val lenT = tBufOpt.map{ tBuf =>
            tBuf.length(q"$element.asInstanceOf[$tpe]") match {
              case Left(c) => q"$c"
              case Right(s) => s
            }
          }.getOrElse(q"0")

          existing match {
            case Some(t) =>
              Some(q"""
              if($element.isInstanceOf[$tpe]) {
                1 + $lenT
              } else {
                $t
              }
            """)
            case None =>
              Some(q"""
              if($element.isInstanceOf[$tpe]) {
              1 + $lenT
            } else {
              sys.error("Did not understand thrift union type")
            }
              """)
          }
      }

      Right(expandedOut.get)
    }

    def genGetFn = {
      val getVal = freshT("getVal")
      val valueA = freshT("valueA")

      val expandedOut: Tree = subData.foldLeft(Option.empty[Tree]) {
        case (existing, (idx, tpe, optiTBuf)) =>
          val extract = optiTBuf.map { tBuf =>
            val (curGetVal, curGetFn) = tBuf.get
            q"""
              val $curGetVal: _root_.java.nio.ByteBuffer = $getVal
              $curGetFn
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
                $t : $outerType
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

      val getFn = q"""
        val $valueA: Int = $getVal.get.toInt
        $expandedOut : $outerType
      """

      (getVal, getFn)
    }

    def genProductMemCompare = {
      import c.universe._

      def freshT(id: String = "CaseClassTerm") = newTermName(c.fresh(s"fresh_$id"))
      val compareInputA = freshT("compareInputA")
      val compareInputB = freshT("compareInputB")

      val arg = freshT("arg")
      val idxCmp = freshT("idxCmp")
      val idxA = freshT("idxA")
      val idxB = freshT("idxB")

      val toIdOpt: Option[Tree] = subData.foldLeft(Option.empty[Tree]) {
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
      }

      val compareSameTypes: Option[Tree] = subData.foldLeft(Option.empty[Tree]) {
        case (existing, (idx, tpe, optiTBuf)) =>
          val commonCmp = optiTBuf.map { tBuf =>
            val (aTerm, bTerm, cmp) = tBuf.compare
            q"""
            val $aTerm: $tpe = $compareInputA.asInstanceOf[$tpe]
            val $bTerm: $tpe = $compareInputB.asInstanceOf[$tpe]
            $cmp : Int
          """
          }.getOrElse(q"0")

          existing match {
            case Some(t) =>
              Some(q"""
              if($idxCmp == $idx) {
                $commonCmp : Int
              } else {
                $t : Int
              }
            """)
            case None =>
              Some(q"""
                if($idxCmp == $idx) {
                  $commonCmp : Int
                } else {
                  sys.error("Unable to compare unknown type")
                }""")
          }
      }

      val compareFn = q"""
        def instanceToIdx($arg: $outerType): Int = {
          ${toIdOpt.get}: Int
        }

        val $idxA: Int = instanceToIdx($compareInputA)
        val $idxB: Int = instanceToIdx($compareInputB)
        val $idxCmp: Int = $idxA.compare($idxB)

        if($idxCmp != 0) {
          $idxCmp: Int
        } else {
          ${compareSameTypes.get}: Int
        }
      """

      (compareInputA, compareInputB, compareFn)
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genBinaryCompare
      override val hash = genHashFn
      override val put = genPutFn
      override val get = genGetFn
      override val compare = genProductMemCompare
      override def length(element: Tree) = genLength(element)
    }
  }
}

