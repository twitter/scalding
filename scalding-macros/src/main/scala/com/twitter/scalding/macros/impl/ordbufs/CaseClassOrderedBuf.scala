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
import com.twitter.bijection.macros.impl.IsCaseClassImpl

object CaseClassOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass && !tpe.typeConstructor.takesTypeArgs =>
      CaseClassOrderedBuf(c)(buildDispatcher, tpe)
  }

  def genProductBinaryCompare(c: Context)(elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]) = {
    import c.universe._
    def freshT(id: String = "CaseClassTerm") = newTermName(c.fresh(s"fresh_$id"))

    val bbA = freshT("bbA")
    val bbB = freshT("bbA")
    val binaryCmpTree = elementData.foldLeft(Option.empty[Tree]) {
      case (existingTreeOpt, (tpe, accessorSymbol, tBuf)) =>
        val (aTerm, bTerm, cmp) = tBuf.compareBinary
        val curCmp = freshT("curCmp")
        val cmpTree = q"""
          val $aTerm = $bbA
          val $bTerm = $bbB
          $cmp
          """
        existingTreeOpt match {
          case Some(t) =>
            val lastCmp = freshT("lastCmp")
            Some(q"""
              val $lastCmp = $t
              if($lastCmp != 0) {
                $lastCmp
              } else {
                $cmpTree
              }
              """)
          case None =>
            Some(cmpTree)
        }
    }.getOrElse(q"0")
    (bbA, bbB, binaryCmpTree)
  }

  def genProductPut(c: Context)(elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]) = {
    import c.universe._
    def freshT(id: String = "CaseClassTerm") = newTermName(c.fresh(s"fresh_$id"))

    val outerBB = freshT("outerCCBB")
    val outerArg = freshT("outerArg")

    val outerPutFn = elementData.foldLeft(q"") {
      case (existingTree, (tpe, accessorSymbol, tBuf)) =>
        val (innerBB, innerArg, innerPutFn) = tBuf.put
        q"""
          $existingTree
          val $innerBB = $outerBB
          val $innerArg = $outerArg.$accessorSymbol
          $innerPutFn
          """
    }
    (outerBB, outerArg, outerPutFn)
  }

  def genProductLength(c: Context)(elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])],
    element: c.universe.Tree) = {
    import c.universe._

    val (constElements, nonConst) = elementData.map {
      case (tpe, accessorSymbol, tBuf) =>
        tBuf.length(q"$element.$accessorSymbol")
    }.partition(_.isLeft)

    val constTotal = constElements.collect {
      case Left(s) => s
    }.reduceLeftOption(_ + _).getOrElse(0)

    if (nonConst.isEmpty) {
      Left(constTotal)
    } else {
      Right(nonConst.foldLeft(q"""$constTotal""") {
        case (_, Left(_)) => sys.error("Invalid match")
        case (prev, Right(t)) =>
          q"$prev + $t"
      })
    }
  }

  def genProductMemCompare(c: Context)(elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]) = {
    import c.universe._

    def freshT(id: String = "CaseClassTerm") = newTermName(c.fresh(s"fresh_$id"))
    val compareInputA = freshT("compareInputA")
    val compareInputB = freshT("compareInputB")
    val compareFn = elementData.foldLeft(Option.empty[Tree]) {
      case (existingTreeOpt, (tpe, accessorSymbol, tBuf)) =>
        val (aTerm, bTerm, cmp) = tBuf.compare
        val curCmp = freshT("curCmp")
        val cmpTree = q"""
            val $aTerm = $compareInputA.$accessorSymbol
            val $bTerm = $compareInputB.$accessorSymbol
                $cmp
          """
        existingTreeOpt match {
          case Some(t) =>
            val lastCmp = freshT("lastCmp")
            Some(q"""
                  val $lastCmp = $t
                if($lastCmp != 0) {
                  $lastCmp
                } else {
                  $cmpTree
                }
              """)
          case None =>
            Some(cmpTree)
        }
    }.getOrElse(q"0")

    (compareInputA, compareInputB, compareFn)
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String = "CaseClassTerm") = newTermName(c.fresh(s"fresh_$id"))

    val dispatcher = buildDispatcher
    val elementData: List[(c.universe.Type, TermName, TreeOrderedBuf[c.type])] =
      outerType
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { accessorMethod =>
          val fieldType = accessorMethod.returnType
          val b: TreeOrderedBuf[c.type] = dispatcher(fieldType)
          (fieldType, accessorMethod.name.toTermName, b)
        }.toList

    def genHashFn = {
      val hashVal = freshT("hashVal")
      val hashFn = q"$hashVal.hashCode"
      (hashVal, hashFn)
    }

    def genGetFn = {
      val getVal = freshT("getVal")
      val getValProcessor = elementData.map {
        case (tpe, accessorSymbol, tBuf) =>
          val (curGetVal, curGetFn) = tBuf.get
          val curR = freshT("curR")
          val builderTree = q"""
          val $curR = {
            val $curGetVal = $getVal
            $curGetFn
          }
        """
          (builderTree, curR)
      }
      val getValTree = q"""
       ..${getValProcessor.map(_._1)}
       ${outerType.typeSymbol.companionSymbol}(..${getValProcessor.map(_._2)})
        """
      (getVal, getValTree)
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genProductBinaryCompare(c)(elementData)
      override val hash = genHashFn
      override val put = genProductPut(c)(elementData)
      override val get = genGetFn
      override val compare = genProductMemCompare(c)(elementData)
      override val lazyOuterVariables: Map[String, ctx.Tree] = elementData.map(_._3.lazyOuterVariables).reduce(_ ++ _)
      override def length(element: Tree) = CaseClassOrderedBuf.genProductLength(c)(elementData, element)
    }
  }
}

