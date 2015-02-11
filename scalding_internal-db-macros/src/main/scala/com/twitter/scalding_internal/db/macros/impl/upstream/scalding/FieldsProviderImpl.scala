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
package com.twitter.scalding_internal.db.macros.impl.upstream.scalding

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.scalding._
import com.twitter.scalding_internal.db.macros.upstream.bijection.IsCaseClass
import com.twitter.scalding_internal.db.macros.impl.upstream.bijection.IsCaseClassImpl

object NamingScheme extends Enumeration {
  type NamingScheme = Value
  val Indexed, NamedWithPrefix, NamedNoPrefix = Value
}
import NamingScheme._

private[macros] object FieldsProviderImpl {
  def toFieldsImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] =
    toFieldsCommonImpl(c, NamedWithPrefix, false)(T)

  def toFieldsWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] =
    toFieldsCommonImpl(c, NamedWithPrefix, true)(T)

  def toFieldsWithUnknownNoPrefixImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] =
    toFieldsCommonImpl(c, NamedNoPrefix, true)(T)

  def toIndexedFieldsImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] =
    toFieldsCommonImpl(c, Indexed, false)(T)

  def toIndexedFieldsWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] =
    toFieldsCommonImpl(c, Indexed, true)(T)

  def toFieldsCommonImpl[T](c: Context, namingScheme: NamingScheme, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    def matchField(fieldType: Type, outerName: Option[String], fieldName: String, isOption: Boolean): List[(Tree, String)] = {
      val returningType = if (isOption) q"""classOf[java.lang.Object]""" else q"""classOf[$fieldType]"""
      val simpleRet = outerName match {
        case Some(outer) => List((returningType, s"$outer$fieldName"))
        case None => List((returningType, s"$fieldName"))
      }
      fieldType match {
        case tpe if tpe =:= typeOf[String] => simpleRet
        case tpe if tpe =:= typeOf[Boolean] => simpleRet
        case tpe if tpe =:= typeOf[Short] => simpleRet
        case tpe if tpe =:= typeOf[Int] => simpleRet
        case tpe if tpe =:= typeOf[Long] => simpleRet
        case tpe if tpe =:= typeOf[Float] => simpleRet
        case tpe if tpe =:= typeOf[Double] => simpleRet
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && isOption == true => c.abort(c.enclosingPosition, s"Case class ${T} has nested options, not supported currently.")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          matchField(innerType, outerName, fieldName, true)
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
          val prefix = outerName.map(pre => s"$pre$fieldName.")
          expandMethod(tpe, prefix, isOption)
        case tpe if allowUnknownTypes => simpleRet
        case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, outerName: Option[String], isOption: Boolean): List[(Tree, String)] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .flatMap { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          matchField(fieldType, outerName, fieldName, isOption)
        }.toList
    }

    val prefix = if (namingScheme == NamedNoPrefix) None else Some("")
    val expanded = expandMethod(T.tpe, prefix, false)
    if (expanded.isEmpty) c.abort(c.enclosingPosition, s"Case class ${T} has no primitive types we were able to extract")

    val typeTrees = expanded.map(_._1)

    val res = if (namingScheme == NamedWithPrefix || namingScheme == NamedNoPrefix) {
      val fieldNames = expanded.map(_._2)
      q"""
      new _root_.cascading.tuple.Fields(_root_.scala.Array.apply[java.lang.Comparable[_]](..$fieldNames),
        _root_.scala.Array.apply[java.lang.reflect.Type](..$typeTrees)) with _root_.com.twitter.scalding_internal.db.macros.upstream.bijection.MacroGenerated
      """
    } else {
      val indices = typeTrees.zipWithIndex.map(_._2)
      q"""
      new _root_.cascading.tuple.Fields(_root_.scala.Array.apply[_root_.java.lang.Comparable[_]](..$indices),
        _root_.scala.Array.apply[java.lang.reflect.Type](..$typeTrees)) with _root_.com.twitter.scalding_internal.db.macros.upstream.bijection.MacroGenerated
      """
    }
    c.Expr[cascading.tuple.Fields](res)
  }

}
