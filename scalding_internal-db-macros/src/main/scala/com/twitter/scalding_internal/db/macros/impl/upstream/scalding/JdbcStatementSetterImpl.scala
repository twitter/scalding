/*
 Copyright 2015 Twitter, Inc.

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
package com.twitter.scalding_internal.db.macros.impl.upstream.scalding

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.scalding_internal.db.JdbcStatementSetter
import com.twitter.scalding_internal.db.macros.upstream.bijection.{ IsCaseClass, MacroGenerated }
import com.twitter.scalding_internal.db.macros.impl.upstream.bijection.IsCaseClassImpl

private[macros] object JdbcStatementSetterImpl {

  def caseClassJdbcStatementSetterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[JdbcStatementSetter[T]] =
    caseClassJdbcStatementSetterCommonImpl(c, false)

  def caseClassJdbcStatementSetterWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[JdbcStatementSetter[T]] =
    caseClassJdbcStatementSetterCommonImpl(c, true)

  def caseClassJdbcStatementSetterCommonImpl[T](c: Context, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[JdbcStatementSetter[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    def matchField(outerTpe: Type, idx: Int, pTree: Tree): (Int, Tree) = {
      def simpleType(accessor: Tree) =
        (idx + 1, q"""${accessor}(${idx + 1}, $pTree)""")

      outerTpe match {
        case tpe if tpe =:= typeOf[String] => simpleType(q"stmt.setString")
        case tpe if tpe =:= typeOf[Boolean] => simpleType(q"stmt.setBoolean")
        case tpe if tpe =:= typeOf[Short] => simpleType(q"stmt.setShort")
        case tpe if tpe =:= typeOf[Int] => simpleType(q"stmt.setInt")
        case tpe if tpe =:= typeOf[Long] => simpleType(q"stmt.setLong")
        case tpe if tpe =:= typeOf[Float] => simpleType(q"stmt.setFloat")
        case tpe if tpe =:= typeOf[Double] => simpleType(q"stmt.setDouble")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val cacheName = newTermName(c.fresh(s"optiIndx"))
          val (newIdx, subTree) =
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, idx, q"$cacheName")
          val nullSetters = (idx until newIdx).map { curIdx =>
            q"""stmt.setObject(${curIdx + 1}, null)"""
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
        case tpe if allowUnknownTypes => simpleType(q"stmt.setObject")
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
    new _root_.com.twitter.scalding_internal.db.JdbcStatementSetter[$T] with _root_.com.twitter.scalding_internal.db.macros.upstream.bijection.MacroGenerated {
      override def apply(t: $T, stmt: _root_.java.sql.PreparedStatement) = {
        $set
      }
    }
    """
    c.Expr[JdbcStatementSetter[T]](res)
  }
}
