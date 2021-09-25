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

import scala.reflect.macros.blackbox.Context

object SealedTraitOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if (tpe.typeSymbol.isClass && (tpe.typeSymbol.asClass.isAbstractClass || tpe.typeSymbol.asClass.isTrait)) =>
        SealedTraitOrderedBuf(c)(buildDispatcher, tpe)
    }
    pf
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]],
    outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = TermName(c.freshName(s"$id"))

    val knownDirectSubclasses = outerType.typeSymbol.asClass.knownDirectSubclasses

    if (knownDirectSubclasses.isEmpty)
      sys.error(
        s"Unable to access any knownDirectSubclasses for $outerType , a bug in scala 2.10/2.11 makes this unreliable. -- ${c.enclosingPosition}")

    // 22 is a magic number, so pick it aligning with usual size for case class fields
    // could be bumped, but the getLength method may get slow, or fail to compile at some point.
    if (knownDirectSubclasses.size > 22)
      sys.error(
        s"More than 22 subclasses($outerType). This code is inefficient for this and may cause jvm errors. Supply code manually. -- ${c.enclosingPosition}")

    val subClassesValid = knownDirectSubclasses.forall { sc =>
      scala.util.Try(sc.asType.asClass.isCaseClass).getOrElse(false)
    }

    if (!subClassesValid)
      sys.error(
        s"We only support the extension of a sealed trait with case classes, for type $outerType -- ${c.enclosingPosition}")

    val dispatcher = buildDispatcher

    val subClasses: List[Type] =
      knownDirectSubclasses.map(_.asType.toType).toList.sortBy(_.toString)

    val subData: List[(Int, Type, TreeOrderedBuf[c.type])] = subClasses
      .map { t =>
        (t, dispatcher(t))
      }
      .zipWithIndex
      .map { case ((tpe, tbuf), idx) => (idx, tpe, tbuf) }

    require(subData.nonEmpty,
      "Unable to parse any subtypes for the sealed trait, error. This must be an error.")

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) =
        SealedTraitLike.compareBinary(c)(inputStreamA, inputStreamB)(subData)
      override def hash(element: ctx.TermName): ctx.Tree =
        SealedTraitLike.hash(c)(element)(subData)
      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        SealedTraitLike.put(c)(inputStream, element)(subData)
      override def get(inputStream: ctx.TermName): ctx.Tree =
        SealedTraitLike.get(c)(inputStream)(subData)
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree =
        SealedTraitLike.compare(c)(outerType, elementA, elementB)(subData)
      override def length(element: Tree): CompileTimeLengthTypes[c.type] =
        SealedTraitLike.length(c)(element)(subData)
      override val lazyOuterVariables: Map[String, ctx.Tree] =
        subData.map(_._3.lazyOuterVariables).reduce(_ ++ _)
    }
  }
}
