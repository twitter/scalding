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

object ProductOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._
    val validTypes: List[Type] = List(typeOf[Product1[Any]],
      typeOf[Product2[Any, Any]],
      typeOf[Product3[Any, Any, Any]],
      typeOf[Product4[Any, Any, Any, Any]],
      typeOf[Product5[Any, Any, Any, Any, Any]],
      typeOf[Product6[Any, Any, Any, Any, Any, Any]],
      typeOf[Product7[Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product8[Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product9[Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Product22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]])

    def validType(curType: Type): Boolean = {
      validTypes.find{ t => curType <:< t }.isDefined
    }

    def symbolFor(subType: Type): Type = {
      val superType = validTypes.find{ t => subType.erasure <:< t }.get
      subType.baseType(superType.typeSymbol)
    }

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if validType(tpe.erasure) => ProductOrderedBuf(c)(buildDispatcher, tpe, symbolFor(tpe))
    }
    pf
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], originalType: c.Type, outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT = newTermName(c.fresh(s"fresh_Product"))
    def freshNT(id: String = "Product") = newTermName(c.fresh(s"fresh_$id"))

    val dispatcher = buildDispatcher
    val elementData: List[(c.universe.Type, MethodSymbol, TreeOrderedBuf[c.type])] =
      outerType
        .declarations
        .collect { case m: MethodSymbol => m }
        .filter(m => m.name.toTermName.toString.startsWith("_"))
        .map { accessorMethod =>
          val fieldType = accessorMethod.returnType.asSeenFrom(outerType, outerType.typeSymbol.asClass)
          val b: TreeOrderedBuf[c.type] = dispatcher(fieldType)
          (fieldType, accessorMethod, b)
        }.toList

    def genHashFn = {
      val hashVal = freshT
      val hashFn = q"$hashVal.hashCode"
      (hashVal, hashFn)
    }

    def genGetFn = {
      val getVal = freshNT("getVal")
      val getValProcessor = elementData.map {
        case (tpe, accessorSymbol, tBuf) =>
          val (curGetVal, curGetFn) = tBuf.get
          val curR = freshNT("curR")
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
       new ${originalType}(..${getValProcessor.map(_._2)})
        """
      (getVal, getValTree)
    }

    def genPutFn = {

      val outerBB = freshNT("outerBB")
      val outerArg = freshNT("outerArg")

      val outerPutFn = elementData.foldLeft(q"") {
        case (existingTree, (tpe, accessorSymbol, tBuf)) =>
          val (innerBB, innerArg, innerPutFn) = tBuf.put
          val curCmp = freshNT("curCmp")
          q"""
          $existingTree
          val $innerBB = $outerBB
          val $innerArg = $outerArg.$accessorSymbol
          $innerPutFn
          """
      }
      (outerBB, outerArg, outerPutFn)
    }

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override val compareBinary = CaseClassOrderedBuf.genProductBinaryCompare(c)(elementData)
      override val hash = genHashFn
      override val put = genPutFn
      override val get = genGetFn
      override val compare = CaseClassOrderedBuf.genProductMemCompare(c)(elementData)
    }
  }
}

