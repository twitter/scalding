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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization.providers

import com.twitter.scalding.serialization.macros.impl.ordered_serialization._

import scala.language.experimental.macros
import scala.reflect.macros.Context

object SealedTraitOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if (tpe.typeSymbol.isClass && (tpe.typeSymbol.asClass.isAbstractClass || tpe.typeSymbol.asClass.isTrait)) => SealedTraitOrderedBuf(c)(buildDispatcher, tpe)
    }
    pf
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(s"$id"))

    val knownDirectSubclasses = StableKnownDirectSubclasses(c)(outerType)

    if (knownDirectSubclasses.isEmpty)
      c.abort(c.enclosingPosition, s"Unable to access any knownDirectSubclasses for $outerType , a bug in scala 2.10/2.11 makes this unreliable.")

    val subClassesValid = knownDirectSubclasses.forall { sc =>
      scala.util.Try(sc.asType.asClass.isCaseClass).getOrElse(false)
    }

    if (!subClassesValid)
      c.abort(c.enclosingPosition, "We only support the extension of a sealed trait with case classes.")

    val dispatcher = buildDispatcher

    val subClasses: List[Type] = knownDirectSubclasses.map(_.asType.toType).toList

    val subData: List[(Int, Type, TreeOrderedBuf[c.type])] = subClasses.map { t =>
      (t, dispatcher(t))
    }.zipWithIndex.map{ case ((tpe, tbuf), idx) => (idx, tpe, tbuf) }.toList

    require(subData.nonEmpty, "Unable to parse any subtypes for the sealed trait, error. This must be an error.")

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = SealedTraitLike.compareBinary(c)(inputStreamA, inputStreamB)(subData)
      override def hash(element: ctx.TermName): ctx.Tree =
        SealedTraitLike.hash(c)(element)(subData)
      override def put(inputStream: ctx.TermName, element: ctx.TermName) = SealedTraitLike.put(c)(inputStream, element)(subData)
      override def get(inputStream: ctx.TermName): ctx.Tree = SealedTraitLike.get(c)(inputStream)(subData)
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree = SealedTraitLike.compare(c)(outerType, elementA, elementB)(subData)
      override def length(element: Tree): CompileTimeLengthTypes[c.type] = SealedTraitLike.length(c)(element)(subData)
      override val lazyOuterVariables: Map[String, ctx.Tree] =
        subData.map(_._3.lazyOuterVariables).reduce(_ ++ _)
    }
  }
}

