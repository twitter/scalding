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

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.bijection.macros.impl.IsCaseClassImpl
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object TupleSetterImpl {

  def caseClassTupleSetterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] =
    caseClassTupleSetterCommonImpl(c, false)

  def caseClassTupleSetterWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] =
    caseClassTupleSetterCommonImpl(c, true)

  def caseClassTupleSetterCommonImpl[T](c: Context, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    def matchField(outerTpe: Type, idx: Int, pTree: Tree): (Int, Tree) = {
      def simpleType(accessor: Tree) =
        (idx + 1, q"""${accessor}(${idx}, $pTree)""")

      outerTpe match {
        case tpe if tpe =:= typeOf[String] => simpleType(q"tup.setString")
        case tpe if tpe =:= typeOf[Boolean] => simpleType(q"tup.setBoolean")
        case tpe if tpe =:= typeOf[Short] => simpleType(q"tup.setShort")
        case tpe if tpe =:= typeOf[Int] => simpleType(q"tup.setInteger")
        case tpe if tpe =:= typeOf[Long] => simpleType(q"tup.setLong")
        case tpe if tpe =:= typeOf[Float] => simpleType(q"tup.setFloat")
        case tpe if tpe =:= typeOf[Double] => simpleType(q"tup.setDouble")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val cacheName = newTermName(c.fresh(s"optiIndx"))
          val (newIdx, subTree) =
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, idx, q"$cacheName")
          val nullSetters = (idx until newIdx).map { curIdx =>
            q"""tup.set($curIdx, null)"""
          }

          (newIdx, q"""
            if($pTree.isDefined) {
              val $cacheName = $pTree.get
              $subTree
            } else {
              ..$nullSetters
            }
            """)

        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(tpe, idx, pTree)
        case tpe if allowUnknownTypes => simpleType(q"tup.set")
        case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives, Option of a primitive nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, parentIdx: Int, pTree: Tree): (Int, Tree) =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .foldLeft((parentIdx, q"")) {
          case ((idx, existingTree), accessorMethod) =>
            val (newIdx, subTree) = matchField(accessorMethod.returnType, idx, q"""$pTree.$accessorMethod""")
            (newIdx, q"""
              $existingTree
              $subTree""")
        }

    val (finalIdx, set) = expandMethod(T.tpe, 0, q"t")
    if (finalIdx == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")
    val res = q"""
    new _root_.com.twitter.scalding.TupleSetter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override def apply(t: $T): _root_.cascading.tuple.Tuple = {
        val tup = _root_.cascading.tuple.Tuple.size($finalIdx)
        $set
        tup
      }
      override val arity: _root_.scala.Int = $finalIdx
    }
    """
    c.Expr[TupleSetter[T]](res)
  }
}
