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

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

import com.twitter.scalding._
import com.twitter.scalding.serialization.macros.impl.ordered_serialization.{
  CompileTimeLengthTypes,
  ProductLike,
  TreeOrderedBuf
}
import CompileTimeLengthTypes._
import com.twitter.scalding.serialization.OrderedSerialization

object CaseObjectOrderedBuf {
  def dispatch(c: Context)(): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass && tpe.typeSymbol.asClass.isModuleClass && !tpe.typeConstructor.takesTypeArgs =>
      CaseObjectOrderedBuf(c)(tpe)
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = q"0"

      override def hash(element: ctx.TermName): ctx.Tree = q"${outerType.toString.hashCode}"

      override def put(inputStream: ctx.TermName, element: ctx.TermName) = q"()"

      override def get(inputStream: ctx.TermName): ctx.Tree =
        q"${outerType.typeSymbol.companionSymbol}"

      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree = q"0"

      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
      override def length(element: Tree) = ConstantLengthCalculation(c)(0)
    }
  }
}
