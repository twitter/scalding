/*
 Copyright 2015 Twitter, Inc.

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

import com.twitter.scalding._
import com.twitter.scalding.serialization.macros.impl.ordered_serialization.{ CompileTimeLengthTypes, ProductLike, TreeOrderedBuf }
import CompileTimeLengthTypes._
import com.twitter.scalding.serialization.OrderedSerialization

object EitherOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.erasure =:= c.universe.typeOf[Either[Any, Any]] => EitherOrderedBuf(c)(buildDispatcher, tpe)
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))
    val dispatcher = buildDispatcher

    val leftType = outerType.asInstanceOf[TypeRefApi].args(0) // linter:ignore
    val rightType = outerType.asInstanceOf[TypeRefApi].args(1)
    val leftBuf: TreeOrderedBuf[c.type] = dispatcher(leftType)
    val rightBuf: TreeOrderedBuf[c.type] = dispatcher(rightType)

    def genBinaryCompare(inputStreamA: TermName, inputStreamB: TermName) = {
      val valueOfA = freshT("valueOfA")
      val valueOfB = freshT("valueOfB")
      val tmpHolder = freshT("tmpHolder")
      q"""
        val $valueOfA = $inputStreamA.readByte
        val $valueOfB = $inputStreamB.readByte
        val $tmpHolder = _root_.java.lang.Byte.compare($valueOfA, $valueOfB)
        if($tmpHolder != 0) {
          //they are different, return comparison on type
          $tmpHolder
        } else if($valueOfA == (0: _root_.scala.Byte)) {
          // they are both Left:
          ${leftBuf.compareBinary(inputStreamA, inputStreamB)}
        } else {
          // they are both Right:
          ${rightBuf.compareBinary(inputStreamA, inputStreamB)}
        }
      """
    }

    def genHashFn(element: TermName) = {
      val innerValue = freshT("innerValue")
      q"""
        if($element.isLeft) {
          val $innerValue = $element.left.get
          val x = ${leftBuf.hash(innerValue)}
          // x * (2^31 - 1) which is a mersenne prime
          (x << 31) - x
        }
        else {
          val $innerValue = $element.right.get
          // x * (2^19 - 1) which is a mersenne prime
          val x = ${rightBuf.hash(innerValue)}
          (x << 19) - x
        }
      """
    }

    def genGetFn(inputStreamA: TermName) = {
      val tmpGetHolder = freshT("tmpGetHolder")
      q"""
        val $tmpGetHolder = $inputStreamA.readByte
        if($tmpGetHolder == (0: _root_.scala.Byte)) Left(${leftBuf.get(inputStreamA)})
        else Right(${rightBuf.get(inputStreamA)})
      """
    }

    def genPutFn(inputStream: TermName, element: TermName) = {
      val tmpPutVal = freshT("tmpPutVal")
      val innerValue = freshT("innerValue")
      q"""
        if($element.isRight) {
          $inputStream.writeByte(1: _root_.scala.Byte)
          val $innerValue = $element.right.get
          ${rightBuf.put(inputStream, innerValue)}
        } else {
          $inputStream.writeByte(0: _root_.scala.Byte)
          val $innerValue = $element.left.get
          ${leftBuf.put(inputStream, innerValue)}
        }
      """
    }

    def genCompareFn(elementA: TermName, elementB: TermName) = {
      val aIsRight = freshT("aIsRight")
      val bIsRight = freshT("bIsRight")
      val innerValueA = freshT("innerValueA")
      val innerValueB = freshT("innerValueB")
      q"""
        val $aIsRight = $elementA.isRight
        val $bIsRight = $elementB.isRight
        if(!$aIsRight) {
          if (!$bIsRight) {
            val $innerValueA = $elementA.left.get
            val $innerValueB = $elementB.left.get
            ${leftBuf.compare(innerValueA, innerValueB)}
          }
          else -1 // Left(_) < Right(_)
        }
        else {
          if(!$bIsRight) 1 // Right(_) > Left(_)
          else { // both are right
            val $innerValueA = $elementA.right.get
            val $innerValueB = $elementB.right.get
            ${rightBuf.compare(innerValueA, innerValueB)}
          }
        }
      """
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: TermName, inputStreamB: TermName) = genBinaryCompare(inputStreamA, inputStreamB)
      override def hash(element: TermName): ctx.Tree = genHashFn(element)
      override def put(inputStream: TermName, element: TermName) = genPutFn(inputStream, element)
      override def get(inputStreamA: TermName): ctx.Tree = genGetFn(inputStreamA)
      override def compare(elementA: TermName, elementB: TermName): ctx.Tree = genCompareFn(elementA, elementB)
      override val lazyOuterVariables: Map[String, ctx.Tree] =
        rightBuf.lazyOuterVariables ++ leftBuf.lazyOuterVariables
      override def length(element: Tree): CompileTimeLengthTypes[c.type] = {

        def tree(ctl: CompileTimeLengthTypes[_]): c.Tree = ctl.asInstanceOf[CompileTimeLengthTypes[c.type]].t
        val dyn = q"""_root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen"""

        (leftBuf.length(q"$element.left.get"), rightBuf.length(q"$element.right.get")) match {
          case (lconst: ConstantLengthCalculation[_], rconst: ConstantLengthCalculation[_]) if lconst.toInt == rconst.toInt =>
            // We got lucky, they are the same size:
            ConstantLengthCalculation(c)(1 + rconst.toInt)
          case (_: NoLengthCalculationAvailable[_], _) => NoLengthCalculationAvailable(c)
          case (_, _: NoLengthCalculationAvailable[_]) => NoLengthCalculationAvailable(c)
          case (left: MaybeLengthCalculation[_], right: MaybeLengthCalculation[_]) =>
            MaybeLengthCalculation(c)(q"""
            if ($element.isLeft) { ${tree(left)} + $dyn(1) }
            else { ${tree(right)} + $dyn(1) }
          """)
          case (left: MaybeLengthCalculation[_], right) =>
            MaybeLengthCalculation(c)(q"""
            if ($element.isLeft) { ${tree(left)} + $dyn(1) }
            else { $dyn(${tree(right)}) + $dyn(1) }
          """)
          case (left, right: MaybeLengthCalculation[_]) =>
            MaybeLengthCalculation(c)(q"""
            if ($element.isLeft) { $dyn(${tree(left)}) + $dyn(1) }
            else { ${tree(right)} + $dyn(1) }
          """)
          // Rest are constant, but different values or fast. So the result is fast
          case (left, right) =>
            // They are different sizes. :(
            FastLengthCalculation(c)(q"""
            if($element.isLeft) { 1 + ${tree(left)} }
            else { 1 + ${tree(right)} }
            """)
        }
      }
    }
  }
}

