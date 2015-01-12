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
import com.twitter.scrooge.ThriftStruct
import com.twitter.scalding.macros.impl.ordbufs._

object ScroogeOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if tpe <:< typeOf[ThriftStruct] => ScroogeOrderedBuf(c)(buildDispatcher, tpe)
    }
    pf
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String = "Product") = newTermName(c.fresh(s"fresh_$id"))

    val dispatcher = buildDispatcher

    val companionSymbol = outerType.typeSymbol.companionSymbol

    val fieldNames: List[String] = companionSymbol.asModule.moduleClass.asType.toType
      .declarations
      .filter(_.name.decoded.endsWith("Field "))
      .collect{ case s: TermSymbol => s }
      .filter(_.isStatic)
      .filter(_.isVal)
      .map { t =>
        val decodedName = t.name.decoded
        val cased = decodedName.take(1).toLowerCase ++ decodedName.drop(1)
        cased.dropRight(6)
      }.toList

    val elementData: List[(c.universe.Type, MethodSymbol, TreeOrderedBuf[c.type])] =
      outerType
        .declarations
        .collect { case m: MethodSymbol => m }
        .filter(m => fieldNames.contains(m.name.toTermName.toString))
        .map { accessorMethod =>
          val fieldType = accessorMethod.returnType.asSeenFrom(outerType, outerType.typeSymbol.asClass)
          val b: TreeOrderedBuf[c.type] = dispatcher(fieldType)
          (fieldType, accessorMethod, b)
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
       ${companionSymbol}(..${getValProcessor.map(_._2)})
        """
      (getVal, getValTree)
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = CaseClassOrderedBuf.genProductBinaryCompare(c)(elementData)
      override val hash = genHashFn
      override val put = CaseClassOrderedBuf.genProductPut(c)(elementData)
      override val get = genGetFn
      override val compare = CaseClassOrderedBuf.genProductMemCompare(c)(elementData)
    }
  }
}

