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

    def maybeHandlePrimitive(outerOuterTpe: Type): Option[(Int, Tree)] = {
      def innerLoop(outerTpe: Type, pTree: Tree): Option[Tree] = {
        val typedSetter: scala.util.Try[c.Tree] = fsetter.from(c)(outerTpe, 0, container, pTree)

        (outerTpe, typedSetter) match {
          case (_, Success(setter)) => Some(setter)

          case (tpe, _) if tpe.erasure =:= typeOf[Option[Any]] =>
            val cacheName = newTermName(c.fresh(s"optiIndx"))
            val optSubTree = innerLoop(tpe.asInstanceOf[TypeRefApi].args.head, q"$cacheName")
            val nullSetter = fsetter.absent(c)(0, container)
            optSubTree.map { subTree =>
              q"""
            if($pTree.isDefined) {
              val $cacheName = $pTree.get
              $subTree
            } else {
              $nullSetter
            }
            """
            }
          case _ => None
        }
      }

      innerLoop(outerOuterTpe, q"t").map { resTree =>
        (1, resTree)
      }
    }
    maybeHandlePrimitive(T.tpe).getOrElse {
      if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
        c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type. Issue when building Setter.""")

      @annotation.tailrec
      def normalized(tpe: Type): Type = {
        val norm = tpe.normalize
        if (!(norm =:= tpe))
          normalized(norm)
        else
          tpe
      }

      def matchField(outerTpe: Type, idx: Int, pTree: Tree): (Int, Tree) = {
        val typedSetter: scala.util.Try[c.Tree] = fsetter.from(c)(outerTpe, idx, container, pTree)
        (outerTpe, typedSetter) match {
          case (_, Success(setter)) =>
            // use type-specific setter if present
            (idx + 1, setter)
          case (tpe, _) if tpe.erasure =:= typeOf[Option[Any]] =>
            val cacheName = newTermName(c.fresh(s"optiIndx"))
            val (newIdx, subTree) =
              matchField(tpe.asInstanceOf[TypeRefApi].args.head, idx, q"$cacheName")
            val nullSetters = (idx until newIdx).map { curIdx =>
              fsetter.absent(c)(idx, container)
            }

            (newIdx, q"""
            if($pTree.isDefined) {
              val $cacheName = $pTree.get
              $subTree
            } else {
              ..$nullSetters
            }
            """)

          case (tpe, _) if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) => expandMethod(normalized(tpe), idx, pTree)
          case (tpe, _) if allowUnknownTypes => (idx + 1, fsetter.default(c)(idx, container, pTree))
          case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives, Option of a primitive nested case classes, when building Setter")
        }
      }

      def expandMethod(outerTpe: Type, parentIdx: Int, pTree: Tree): (Int, Tree) =
        outerTpe
          .declarations
          .collect { case m: MethodSymbol if m.isCaseAccessor => m }
          .foldLeft((parentIdx, q"")) {
            case ((idx, existingTree), accessorMethod) =>
              val fieldType = normalized(accessorMethod.returnType.asSeenFrom(outerTpe, outerTpe.typeSymbol.asClass))

              val (newIdx, subTree) = matchField(fieldType, idx, q"""$pTree.$accessorMethod""")
              (newIdx, q"""
              $existingTree
              $subTree""")
          }

      val (finalIdx, set) = expandMethod(normalized(T.tpe), 0, q"t")
      if (finalIdx == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")
      (finalIdx, set)
    }
  }
}
