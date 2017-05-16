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

import com.twitter.scalding.serialization.macros.impl.ordered_serialization.providers.StableKnownDirectSubclasses
import com.twitter.scalding.serialization.macros.impl.ordered_serialization._
import com.twitter.scrooge.ThriftUnion

import scala.language.experimental.macros
import scala.reflect.macros.Context

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

    val subClasses: List[Type] = StableKnownDirectSubclasses(c)(outerType).map(_.toType)
    
    val subData: List[(Int, Type, Option[TreeOrderedBuf[c.type]])] = subClasses.map { t =>
      if (t.typeSymbol.name.toString == "UnknownUnionField") {
        (t, None)
      } else {
        (t, Some(dispatcher(t)))
      }
    }.zipWithIndex.map{ case ((tpe, tbuf), idx) => (idx, tpe, tbuf) }.toList

    require(subData.size > 0, "Must have some sub types on a union?")

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = UnionLike.compareBinary(c)(inputStreamA, inputStreamB)(subData)
      override def hash(element: ctx.TermName): ctx.Tree =
        UnionLike.hash(c)(element)(subData)
      override def put(inputStream: ctx.TermName, element: ctx.TermName) = UnionLike.put(c)(inputStream, element)(subData)
      override def get(inputStream: ctx.TermName): ctx.Tree = UnionLike.get(c)(inputStream)(subData)
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree = UnionLike.compare(c)(outerType, elementA, elementB)(subData)
      override def length(element: Tree): CompileTimeLengthTypes[c.type] = UnionLike.length(c)(element)(subData)
      override val lazyOuterVariables: Map[String, ctx.Tree] = subData.flatMap(_._3).map(_.lazyOuterVariables).reduce(_ ++ _)
    }
  }
}

