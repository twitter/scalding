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
import scala.reflect.runtime.universe._

import com.twitter.scalding._
import com.twitter.bijection.macros.IsCaseClass
import com.twitter.bijection.macros.impl.IsCaseClassImpl

/**
 * Naming scheme for cascading Tuple fields used by FieldsProviderImpl macro.
 */
sealed trait NamingScheme

/**
 * Uses zero-based indexes for field names.
 */
case object Indexed extends NamingScheme

/**
 * Uses prefixes for naming nested fields.
 * For e.g. for the following nested case class:
 * {{{
 *   case class Outer(id: Long, name: String, details: Inner)
 *   case class Inner(phone: Int)
 * }}}
 * the nested field's name will be "details.phone".
 */
case object NamedWithPrefix extends NamingScheme

/**
 * No prefixes for naming nested fields.
 * For e.g. for the following nested case class:
 * {{{
 *   case class Outer(id: Long, name: String, details: Inner)
 *   case class Inner(phone: Int)
 * }}}
 * the nested field's name will remain "phone".
 *
 * Useful esp. for flattening nested case classes to SQL table columns.
 */
case object NamedNoPrefix extends NamingScheme

/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object FieldsProviderImpl {
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

    /**
     * This returns the a Tree expressing the Class[_] to use for T if T is primitive or Option of
     * primitive
     * If we are in an Option, we return classOf[Object] since otherwise cascading will convert
     * nulls or empty strings to numeric zeros.
     * This is only applied
     */
    val maybeHandlePrimitive: Option[Tree] = {
      def innerLoop(tpe: Type, inOption: Boolean): Option[Tree] = {
        val returningType = if (inOption) q"""classOf[java.lang.Object]""" else q"""classOf[${T.tpe}]"""
        val simpleRet = Some(returningType)

        tpe match {
          case tpe if tpe =:= typeOf[String] => simpleRet
          case tpe if tpe =:= typeOf[Boolean] => simpleRet
          case tpe if tpe =:= typeOf[Short] => simpleRet
          case tpe if tpe =:= typeOf[Int] => simpleRet
          case tpe if tpe =:= typeOf[Long] => simpleRet
          case tpe if tpe =:= typeOf[Float] => simpleRet
          case tpe if tpe =:= typeOf[Double] => simpleRet
          case tpe if tpe.erasure =:= typeOf[Option[Any]] && inOption == true =>
            c.abort(c.enclosingPosition, s"Case class ${T} has nested options, not supported currently.")
          case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
            val innerType = tpe.asInstanceOf[TypeRefApi].args.head
            innerLoop(innerType, true)

          case tpe => None
        }
      }
      innerLoop(T.tpe, false)
    }

    val flattened: Either[List[(Tree, String)], List[(Tree, Int)]] =
      maybeHandlePrimitive
        .map { t => Right(List((t, 0))) }
        .getOrElse {
          if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
            c.abort(c.enclosingPosition,
              s"""|We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call
                  |is possibly enclosed in a class.
                  |This will mean the macro is operating on a non-resolved type. Issue when building Fields Provider.""".stripMargin)

          /**
           * This returns a List of pairs which flatten fieldType into (class, name) pairs
           */
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
              case tpe if tpe.erasure =:= typeOf[Option[Any]] && isOption == true =>
                c.abort(c.enclosingPosition, s"Case class ${T} has nested options, not supported currently.")
              case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
                val innerType = tpe.asInstanceOf[TypeRefApi].args.head
                matchField(innerType, outerName, fieldName, true)
              case tpe if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) =>
                val prefix = outerName.map(pre => s"$pre$fieldName.")
                expandMethod(tpe, prefix, isOption)
              case tpe if allowUnknownTypes => simpleRet
              case _ =>
                c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives or nested case classes")
            }
          }

          def expandMethod(outerTpe: Type, outerName: Option[String], isOption: Boolean): List[(Tree, String)] =
            outerTpe
              .declarations
              .collect { case m: MethodSymbol if m.isCaseAccessor => m }
              .flatMap { accessorMethod =>
                val fieldName = accessorMethod.name.toTermName.toString
                val fieldType = accessorMethod.returnType.asSeenFrom(outerTpe, outerTpe.typeSymbol.asClass)

                matchField(fieldType, outerName, fieldName, isOption)
              }.toList

          val prefix = if (namingScheme == NamedNoPrefix) None else Some("")
          val expanded = expandMethod(T.tpe, prefix, false)
          if (expanded.isEmpty) c.abort(c.enclosingPosition, s"Case class ${T} has no primitive types we were able to extract")

          Left(expanded)
        }

    val typeTrees = flattened.fold({ list => list.map(_._1) }, { list => list.map(_._1) })
    val namesOrIds = if (namingScheme == NamedWithPrefix || namingScheme == NamedNoPrefix) {
      flattened match {
        case Left(fieldNames) =>
          q"""_root_.scala.Array.apply[_root_.java.lang.Comparable[_]](..${fieldNames.map(_._2)})"""
        case Right(fieldIds) =>
          q"""_root_.scala.Array.apply[_root_.java.lang.Comparable[_]](..${fieldIds.map(_._2)})"""
      }
    } else {
      val indices = typeTrees.zipWithIndex.map(_._2)
      q"""_root_.scala.Array.apply[_root_.java.lang.Comparable[_]](..$indices)"""
    }
    c.Expr[cascading.tuple.Fields](q"""
      new _root_.cascading.tuple.Fields($namesOrIds,
        _root_.scala.Array.apply[_root_.java.lang.reflect.Type](..$typeTrees))
      """)
  }
}
