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
import com.twitter.scrooge.{ ThriftUnion, ThriftStruct }
import com.twitter.scalding.macros.impl.ordbufs._

object ScroogeOuterOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if tpe <:< typeOf[ThriftStruct] && !(tpe <:< typeOf[ThriftUnion]) => ScroogeOuterOrderedBuf(c)(tpe)
    }
    pf
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String = "Product") = newTermName(c.fresh(s"fresh_$id"))

    val variableNameStr = s"`bufferable_${outerType.typeSymbol.fullName}`"
    val variableName = newTermName(variableNameStr)
    val implicitInstanciator = q"""implicitly[_root_.com.twitter.scalding.typed.OrderedBufferable[$outerType]]"""

    def genBinaryCompare = {
      val elementA = freshT("bbA")
      val elementB = freshT("bbB")
      val binaryCompare = q"""
      $variableName.compareBinary($elementA, $elementB).unsafeToInt
      """
      (elementA, elementB, binaryCompare)
    }

    def genGetFn = {

      val getVal = freshT("getVal")
      val resBB = freshT("resBB")
      val resT = freshT("resT")
      val getFn = q"""
      {
        val ($resBB: _root_.java.nio.ByteBuffer, $resT: $outerType)  = $variableName.get($getVal).get
        $getVal.position($resBB.position)
        $resT : $outerType
      }
      """
      (getVal, getFn)

    }

    def genPutFn = {
      val putBBInput = freshT("scrogeOuterPutInput")
      val putBBdataInput = freshT("scrogeOuterPutItemInput")
      val retBB = freshT("retBB")
      val putFn = q"""
        val $retBB: _root_.java.nio.ByteBuffer = $variableName.put($putBBInput, $putBBdataInput)
        $putBBInput.position($retBB.position)
      """
      (putBBInput, putBBdataInput, putFn)
    }

    def genCompareFn = {
      val compareInputA = freshT("compareInputA")
      val compareInputB = freshT("compareInputBtB")

      val compareFn = q"""
      $variableName.compare($compareInputA, $compareInputB)
      """

      (compareInputA, compareInputB, compareFn)
    }

    def genHashFn = {
      val hashVal = freshT("hashVal")
      val hashFn = q"$variableName.hash($hashVal)"
      (hashVal, hashFn)
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genBinaryCompare
      override val hash = genHashFn
      override val put = genPutFn
      override val get = genGetFn
      override val compare = genCompareFn
      override val lazyOuterVariables = Map(variableNameStr -> implicitInstanciator)
      override def length(element: Tree) = {
        Right(q"""
          $variableName.binaryLength($element)
          """)
      }
    }
  }
}

