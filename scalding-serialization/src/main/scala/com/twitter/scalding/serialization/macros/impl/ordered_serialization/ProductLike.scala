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
import scala.reflect.macros.blackbox.Context

import com.twitter.scalding._

object ProductLike {
  def compareBinary(c: Context)(inputStreamA: c.TermName, inputStreamB: c.TermName)(
    elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = TermName(c.freshName(id))

    elementData
      .foldLeft(Option.empty[Tree]) {
        case (existingTreeOpt, (tpe, accessorSymbol, tBuf)) =>
          existingTreeOpt match {
            case Some(t) =>
              val lastCmp = freshT("lastCmp")
              Some(q"""
              val $lastCmp = $t
              if($lastCmp != 0) {
                $lastCmp
              } else {
                ${tBuf.compareBinary(inputStreamA, inputStreamB)}
              }
              """)
            case None =>
              Some(tBuf.compareBinary(inputStreamA, inputStreamB))
          }
      }
      .getOrElse(q"0")
  }

  def hash(c: Context)(element: c.TermName)(
    elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = TermName(c.freshName(id))

    val currentHash = freshT("last")

    val hashUpdates = elementData.map {
      case (tpe, accessorSymbol, tBuf) =>
        val target = freshT("target")
        q"""
          val $target = $element.$accessorSymbol
            $currentHash = _root_.com.twitter.scalding.serialization.MurmurHashUtils.mixH1($currentHash, ${
          tBuf
            .hash(target)
        })
          """
    }

    q"""
      var $currentHash: Int = _root_.com.twitter.scalding.serialization.MurmurHashUtils.seed
      ..${hashUpdates}
      _root_.com.twitter.scalding.serialization.MurmurHashUtils.fmix($currentHash, ${elementData.size})
    """
  }

  def put(c: Context)(inputStream: c.TermName, element: c.TermName)(
    elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._
    def freshT(id: String) = TermName(c.freshName(id))
    val innerElement = freshT("innerElement")

    elementData.foldLeft(q"") {
      case (existingTree, (tpe, accessorSymbol, tBuf)) =>
        q"""
          $existingTree
          val $innerElement = $element.$accessorSymbol
          ${tBuf.put(inputStream, innerElement)}
          """
    }
  }

  def length(c: Context)(element: c.Tree)(
    elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]): CompileTimeLengthTypes[c.type] = {
    import c.universe._
    import CompileTimeLengthTypes._
    val (constSize, dynamicFunctions, maybeLength, noLength) =
      elementData.foldLeft((0, Vector[c.Tree](), Vector[c.Tree](), 0)) {
        case ((constantLength, dynamicLength, maybeLength, noLength),
          (tpe, accessorSymbol, tBuf)) =>
          tBuf.length(q"$element.$accessorSymbol") match {
            case const: ConstantLengthCalculation[_] =>
              (constantLength + const.asInstanceOf[ConstantLengthCalculation[c.type]].toInt,
                dynamicLength,
                maybeLength,
                noLength)
            case f: FastLengthCalculation[_] =>
              (constantLength,
                dynamicLength :+ f.asInstanceOf[FastLengthCalculation[c.type]].t,
                maybeLength,
                noLength)
            case m: MaybeLengthCalculation[_] =>
              (constantLength,
                dynamicLength,
                maybeLength :+ m.asInstanceOf[MaybeLengthCalculation[c.type]].t,
                noLength)
            case _: NoLengthCalculationAvailable[_] =>
              (constantLength, dynamicLength, maybeLength, noLength + 1)
          }
      }

    val combinedDynamic = dynamicFunctions.foldLeft(q"""$constSize""") {
      case (prev, t) =>
        q"$prev + $t"
    }

    if (noLength > 0) {
      NoLengthCalculationAvailable(c)
    } else {
      if (maybeLength.isEmpty && dynamicFunctions.isEmpty) {
        ConstantLengthCalculation(c)(constSize)
      } else {
        if (maybeLength.isEmpty) {
          FastLengthCalculation(c)(combinedDynamic)
        } else {

          val const =
            q"_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.ConstLen"
          val dyn =
            q"_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen"
          val noLen =
            q"_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation"
          // Contains an MaybeLength
          val combinedMaybe: Tree = maybeLength.reduce { (hOpt, nxtOpt) =>
            q"""$hOpt + $nxtOpt"""
          }
          if (dynamicFunctions.nonEmpty || constSize != 0) {
            MaybeLengthCalculation(c)(q"""
            $combinedMaybe match {
              case $const(l) => $dyn(l + $combinedDynamic)
              case $dyn(l) => $dyn(l + $combinedDynamic)
              case $noLen => $noLen
            }
          """)
          } else {
            MaybeLengthCalculation(c)(combinedMaybe)
          }
        }
      }
    }
  }

  def compare(c: Context)(elementA: c.TermName, elementB: c.TermName)(
    elementData: List[(c.universe.Type, c.universe.TermName, TreeOrderedBuf[c.type])]): c.Tree = {
    import c.universe._

    def freshT(id: String) = TermName(c.freshName(id))

    val innerElementA = freshT("innerElementA")
    val innerElementB = freshT("innerElementB")

    elementData
      .map {
        case (tpe, accessorSymbol, tBuf) =>
          val curCmp = freshT("curCmp")
          val cmpTree = q"""
            val $curCmp: Int = {
              val $innerElementA = $elementA.$accessorSymbol
              val $innerElementB = $elementB.$accessorSymbol
              ${tBuf.compare(innerElementA, innerElementB)}
            }
          """
          (cmpTree, curCmp)
      }
      .reverse // go through last to first
      .foldLeft(None: Option[Tree]) {
        case (Some(rest), (tree, valname)) =>
          Some(
            q"""$tree;
              if ($valname != 0) $valname
              else {
                $rest
              }
          """)
        case (None, (tree, valname)) => Some(q"""$tree; $valname""")
      }
      .getOrElse(q"""0""") // all 0 size products are equal
  }
}
