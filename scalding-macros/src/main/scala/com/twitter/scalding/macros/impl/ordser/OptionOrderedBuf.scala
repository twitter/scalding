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
package com.twitter.scalding.macros.impl.ordser

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization

object OptionOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.erasure =:= c.universe.typeOf[Option[Any]] => OptionOrderedBuf(c)(buildDispatcher, tpe)
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))
    val dispatcher = buildDispatcher

    val innerType = outerType.asInstanceOf[TypeRefApi].args.head
    val innerBuf: TreeOrderedBuf[c.type] = dispatcher(innerType)

    def genBinaryCompare(inputStreamA: TermName, inputStreamB: TermName) = {
      val valueOfA = freshT("valueOfA")
      val valueOfB = freshT("valueOfB")
      val tmpHolder = freshT("tmpHolder")
      q"""
        val $valueOfA = $inputStreamA.readByte
        val $valueOfB = $inputStreamB.readByte
        val $tmpHolder = $valueOfA.compare($valueOfB)
        if($tmpHolder != 0 || $valueOfA == (0: Byte)) {
          $tmpHolder
        } else {
          ${innerBuf.compareBinary(inputStreamA, inputStreamB)}
        }
      """
    }

    def genHashFn(element: TermName) = {
      val innerValue = freshT("innerValue")

      q"""
        if($element.isEmpty)
          0
        else {
          val $innerValue = $element.get
          ${innerBuf.hash(innerValue)}
        }
      """
    }

    def genGetFn(inputStreamA: TermName) = {
      val tmpGetHolder = freshT("tmpGetHolder")
      q"""
        val $tmpGetHolder = $inputStreamA.readByte
        if($tmpGetHolder == 0) {
          None
        } else {
          Some(${innerBuf.get(inputStreamA)})
        }
      """
    }

    def genPutFn(inputStream: TermName, element: TermName) = {
      val tmpPutVal = freshT("tmpPutVal")
      val innerValue = freshT("innerValue")

      q"""
        val $tmpPutVal: _root_.scala.Byte = if($element.isDefined) 1 else 0
        $inputStream.writeByte($tmpPutVal)
        if($tmpPutVal == 1) {
        val $innerValue = $element.get
        ${innerBuf.put(inputStream, innerValue)}
      }
      """
    }

    def genCompareFn(elementA: TermName, elementB: TermName) = {
      val aIsDefined = freshT("aIsDefined")
      val bIsDefined = freshT("bIsDefined")
      val innerValueA = freshT("innerValueA")
      val innerValueB = freshT("innerValueB")

      q"""
        val $aIsDefined = $elementA.isDefined
        val $bIsDefined = $elementB.isDefined
        if(!$aIsDefined && !$bIsDefined) {
          0
        } else if(!$aIsDefined && $bIsDefined) {
          -1
        } else if($aIsDefined && !$bIsDefined) {
            1
        } else {
          val $innerValueA = $elementA.get
          val $innerValueB = $elementB.get

          ${innerBuf.compare(innerValueA, innerValueB)}
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
      override val lazyOuterVariables: Map[String, ctx.Tree] = innerBuf.lazyOuterVariables
      override def length(element: Tree): LengthTypes[c.type] = {
        innerBuf.length(q"$element.get") match {
          case const: ConstantLengthCalculation[_] => FastLengthCalculation(c)(q"""
            if($element.isDefined) {
              1 + ${const.toInt}
            } else {
              1
            }
            """)
          case f: FastLengthCalculation[_] =>
            val tmpSubRes = freshT("tmpSubRes")
            FastLengthCalculation(c)(q"""
           if($element.isDefined) {
              1 + ${f.asInstanceOf[FastLengthCalculation[c.type]].t}
            } else {
              1
            }
          """)
          case m: MaybeLengthCalculation[_] =>
            val t = m.asInstanceOf[MaybeLengthCalculation[c.type]].t
            val eitherT = freshT("either")
            MaybeLengthCalculation(c)(q"""
            if($element.isDefined) {
              $t.map { case $eitherT =>
                $eitherT match {
                  case Left(s) => Right(s + 1) :Either[Int, Int]
                  case Right(s) => Right(s + 1) :Either[Int, Int]
                }
              }: Option[Either[Int, Int]]
            } else {
              Some(Right(1)): Option[Either[Int, Int]]
            }
          """)
          case _ => NoLengthCalculationAvailable(c)
        }
      }
    }
  }
}

