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
package com.twitter.scalding.macros.impl

import scala.reflect.macros.Context
import scala.util.{ Failure, Success }

import com.twitter.scalding._
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.bijection.macros.impl.IsCaseClassImpl

/**
 * Helper class for generating setters from case class to
 * other types. E.g. cascading Tuple, jdbc PreparedStatement
 */
object CaseClassBasedSetterImpl {

  def apply[T](c: Context)(container: c.TermName, allowUnknownTypes: Boolean,
    fsetter: CaseClassFieldSetter)(implicit T: c.WeakTypeTag[T]): (Int, c.Tree) = {
    import c.universe._

    sealed trait SetterBuilder {
      def columns: Int
      /**
       * This Tree assumes that "val $value = ..." has been set
       */
      def setTree(value: Tree, offset: Int): Tree
    }
    case class PrimitiveSetter(tpe: Type) extends SetterBuilder {
      def columns = 1
      def setTree(value: Tree, offset: Int) = fsetter.from(c)(tpe, offset, container, value) match {
        case Success(tree) => tree
        case Failure(e) => c.abort(c.enclosingPosition,
          s"Case class ${T} is supported. Error on $tpe, ${e.getMessage}")
      }
    }
    case object DefaultSetter extends SetterBuilder {
      def columns = 1
      def setTree(value: Tree, offset: Int) = fsetter.default(c)(offset, container, value)
    }
    case class OptionSetter(inner: SetterBuilder) extends SetterBuilder {
      def columns = inner.columns
      def setTree(value: Tree, offset: Int) = {
        val someVal = newTermName(c.fresh("someVal"))
        val someValTree = q"$someVal"
        q"""if($value.isDefined) {
          val $someVal = $value.get
          ${inner.setTree(someValTree, offset)}
        } else {
          ${fsetter.absent(c)(offset, container)}
        }"""
      }
    }
    case class CaseClassSetter(members: Vector[(Tree => Tree, SetterBuilder)]) extends SetterBuilder {
      val columns = members.map(_._2.columns).sum
      def setTree(value: Tree, offset: Int) = {
        val setters = members.scanLeft((offset, Option.empty[Tree])) {
          case ((off, _), (access, sb)) =>
            val cca = newTermName(c.fresh("access"))
            val ccaT = q"$cca"
            (off + sb.columns, Some(q"val $cca = ${access(value)}; ${sb.setTree(ccaT, off)}"))
        }
          .collect { case (_, Some(tree)) => tree }
        q"""..$setters"""
      }
    }

    @annotation.tailrec
    def normalized(tpe: Type): Type = {
      val norm = tpe.normalize
      if (!(norm =:= tpe))
        normalized(norm)
      else
        tpe
    }

    def matchField(outerType: Type): SetterBuilder = {
      // we do this just to see if the setter matches.
      val dummyIdx = 0
      val dummyTree = q"t"
      outerType match {
        case tpe if fsetter.from(c)(tpe, dummyIdx, container, dummyTree).isSuccess =>
          PrimitiveSetter(tpe)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          OptionSetter(matchField(innerType))
        case tpe if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) =>
          CaseClassSetter(expandMethod(normalized(tpe)).map {
            case (fn, tpe) =>
              (fn, matchField(tpe))
          })
        case tpe if allowUnknownTypes =>
          DefaultSetter
        case _ =>
          c.abort(c.enclosingPosition,
            s"Case class ${T.tpe} is not supported at type: $outerType")
      }
    }
    def expandMethod(outerTpe: Type): Vector[(Tree => Tree, Type)] =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { accessorMethod =>
          val fieldType = normalized(accessorMethod.returnType.asSeenFrom(outerTpe, outerTpe.typeSymbol.asClass))

          ({ pTree: Tree => q"""$pTree.$accessorMethod""" }, fieldType)
        }
        .toVector

    // in TupleSetterImpl, the outer-most input val is called t, so we pass that in here:
    val sb = matchField(normalized(T.tpe))
    if (sb.columns == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")
    (sb.columns, sb.setTree(q"t", 0))
  }
}
