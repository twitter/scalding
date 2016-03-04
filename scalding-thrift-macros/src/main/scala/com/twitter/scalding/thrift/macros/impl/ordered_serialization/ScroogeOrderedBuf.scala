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

import com.twitter.scalding.serialization.macros.impl.ordered_serialization._
import com.twitter.scrooge.{ ThriftStruct, ThriftUnion }

import scala.language.experimental.macros
import scala.reflect.macros.Context

/*
  This is like the product macros, indeed using most of its code from scalding-macros. Except:
  Scrooge traits don't use the _1, _2, .. fields as the primary fields, they are defined in the trait to point to
  fields named after the thrift fields. So we look at the companion object to figure out those fields names.
  Then we scan the trait for those methods to build the similar listing as is used in products. Other than that we use
  the same constructor approach to case classes in calling the companion object over calling new on the trait
  */
object ScroogeOrderedBuf {
  def dispatch(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]]): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._

    val pf: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
      case tpe if tpe <:< typeOf[ThriftStruct] && !(tpe =:= typeOf[ThriftStruct]) && !(tpe <:< typeOf[ThriftUnion]) => ScroogeOrderedBuf(c)(buildDispatcher, tpe)
    }
    pf
  }

  def apply(c: Context)(buildDispatcher: => PartialFunction[c.Type, TreeOrderedBuf[c.type]], outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(id))

    val dispatcher = buildDispatcher

    val companionSymbol = outerType.typeSymbol.companionSymbol

    val fieldNames: List[String] = companionSymbol.asModule.moduleClass.asType.toType
      .declarations
      .filter(_.name.decoded.endsWith("Field "))
      .collect { case s: TermSymbol => s }
      .filter(_.isStatic)
      .filter(_.isVal)
      .map { t =>
        val decodedName = t.name.decoded // Looks like "MethodNameField "
        decodedName.dropRight(6).toLowerCase //  These things end in "Field " , yes there is a space in there
      }.toList

    val elementData: List[(c.universe.Type, TermName, TreeOrderedBuf[c.type])] =
      outerType
        .declarations
        .collect { case m: MethodSymbol => m }
        .filter(m => fieldNames.contains(m.name.toTermName.toString.toLowerCase))
        .map { accessorMethod =>
          val fieldType = accessorMethod.returnType.asSeenFrom(outerType, outerType.typeSymbol.asClass)
          val b: TreeOrderedBuf[c.type] = dispatcher(fieldType)
          (fieldType, accessorMethod.name.toTermName, b)
        }.toList

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) =
        ProductLike.compareBinary(c)(inputStreamA, inputStreamB)(elementData)

      override def hash(element: ctx.TermName): ctx.Tree = ProductLike.hash(c)(element)(elementData)

      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        ProductLike.put(c)(inputStream, element)(elementData)

      override def get(inputStream: ctx.TermName): ctx.Tree = {

        val getValProcessor = elementData.map {
          case (tpe, accessorSymbol, tBuf) =>
            val curR = freshT("curR")
            val builderTree = q"""
          val $curR = {
            ${tBuf.get(inputStream)}
          }
        """
            (builderTree, curR)
        }
        q"""
       ..${getValProcessor.map(_._1)}
       ${companionSymbol}(..${getValProcessor.map(_._2)}) : $outerType
        """
      }
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree =
        ProductLike.compare(c)(elementA, elementB)(elementData)

      override val lazyOuterVariables: Map[String, ctx.Tree] =
        elementData.map(_._3.lazyOuterVariables).reduceLeftOption(_ ++ _).getOrElse(Map())

      override def length(element: Tree) =
        ProductLike.length(c)(element)(elementData)
    }
  }
}

