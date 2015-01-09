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

object OptionOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.erasure =:= c.universe.typeOf[Option[Any]] => OptionOrderedBuf(c)(buildDispatcher, tpe)
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT = newTermName(c.fresh(s"freshTerm"))
    val dispatcher = buildDispatcher

    val innerType = outerType.asInstanceOf[TypeRefApi].args.head
    val innerBuf: TreeOrderedBuf[c.type] = dispatcher(innerType)

    def genBinaryCompare = {
      val bbA = freshT
      val bbB = freshT
      val tmpHolder = freshT
      val (innerbbA, innerbbB, innerFunc) = innerBuf.compareBinary
      val binaryCompare = q"""
        val $tmpHolder = $bbA.get.compare($bbB.get)
        if($tmpHolder != 0) return $tmpHolder

        val $innerbbA = $bbA
        val $innerbbB = $bbB
        $innerFunc
      """
      (bbA, bbB, binaryCompare)
    }

    def genHashFn = {
      val hashVal = freshT
      val (innerHashVal, innerHashFn) = innerBuf.hash
      val hashFn = q"""
        if($hashVal.isEmpty) return 0
        val $innerHashVal = $hashVal.get
        $innerHashFn
      """
      (hashVal, hashFn)
    }

    def genGetFn = {
      val getVal = freshT
      val (innerGetVal, innerGetFn) = innerBuf.get
      val tmpGetHolder = freshT
      val getFn = q"""
        val $tmpGetHolder = $getVal.get
        if($tmpGetHolder == 0) {
          None
        } else {
          val $innerGetVal = $getVal
          Some($innerGetFn)
        }
      """
      (getVal, getFn)
    }

    def genPutFn = {
      val outerBB = freshT
      val outerInput = freshT
      val (innerBB, innerInput, innerPutFn) = innerBuf.put
      val tmpPutVal = freshT

      val putFn = q"""
        val $tmpPutVal: _root_.scala.Byte = if($outerInput.isDefined) 1 else 0
        $outerBB.put($tmpPutVal)
        if($tmpPutVal == 1) {
          val $innerBB = $outerBB
          val $innerInput = $outerInput.get
          $innerPutFn
        }
      """
      (outerBB, outerInput, putFn)
    }

    def genCompareFn = {
      val inputA = freshT
      val inputB = freshT
      val (innerInputA, innerInputB, innerCompareFn) = innerBuf.compare
      val tmpPutVal = freshT
      val aIsDefined = freshT
      val bIsDefined = freshT
      val compareFn = q"""
        val $aIsDefined = $inputA.isDefined
        val $bIsDefined = $inputB.isDefined
        if(!$aIsDefined && !$bIsDefined) return 0
        if(!$aIsDefined && $bIsDefined) return -1
        if($aIsDefined && !$bIsDefined) return 1

        val $innerInputA = $inputA.get
        val $innerInputB = $inputB.get
        $innerCompareFn
      """

      (inputA, inputB, compareFn)
    }

    val compareInputA = freshT
    val compareInputB = freshT
    val compareFn = q"$compareInputA.compare($compareInputB)"

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = genBinaryCompare
      override val hash = genHashFn
      override val put = genPutFn
      override val get = genGetFn
      override val compare = genCompareFn
    }
  }
}

