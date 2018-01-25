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

import scala.annotation.tailrec
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

    import TypeDescriptorProviderImpl.{ optionInner, evidentColumn }

    @tailrec
    def isNumbered(t: Type): Boolean =
      t match {
        case tpe if tpe =:= typeOf[Boolean] => true
        case tpe if tpe =:= typeOf[Short] => true
        case tpe if tpe =:= typeOf[Int] => true
        case tpe if tpe =:= typeOf[Long] => true
        case tpe if tpe =:= typeOf[Float] => true
        case tpe if tpe =:= typeOf[Double] => true
        case tpe if tpe =:= typeOf[String] => true
        case tpe =>
          optionInner(c)(tpe) match {
            case Some(t) => isNumbered(t)
            case None => false
          }
      }

    object FieldBuilder {
      // This is method on the object to work around this compiler bug: SI-6231
      def toFieldsTree(fb: FieldBuilder, scheme: NamingScheme): Tree = {
        val nameTree = scheme match {
          case Indexed =>
            val indices = fb.names.zipWithIndex.map(_._2)
            q"""_root_.scala.Array.apply[_root_.java.lang.Comparable[_]](..$indices)"""
          case _ =>
            q"""_root_.scala.Array.apply[_root_.java.lang.Comparable[_]](..${fb.names})"""
        }
        q"""new _root_.cascading.tuple.Fields($nameTree,
          _root_.scala.Array.apply[_root_.java.lang.reflect.Type](..${fb.columnTypes}))
         """
      }
    }
    sealed trait FieldBuilder {
      def columnTypes: Vector[Tree]
      def names: Vector[String]
    }
    case class Primitive(name: String, tpe: Type) extends FieldBuilder {
      def columnTypes = Vector(q"""_root_.scala.Predef.classOf[$tpe]""")
      def names = Vector(name)
    }
    case class OptionBuilder(of: FieldBuilder) extends FieldBuilder {
      // Options just use Object as the type, due to the way cascading works on number types
      def columnTypes = of.columnTypes.map(_ => q"""_root_.scala.Predef.classOf[_root_.java.lang.Object]""")
      def names = of.names
    }
    case class CaseClassBuilder(prefix: String, members: Vector[FieldBuilder]) extends FieldBuilder {
      def columnTypes = members.flatMap(_.columnTypes)
      def names = for {
        member <- members
        name <- member.names
      } yield if (namingScheme == NamedWithPrefix && prefix.nonEmpty) s"$prefix.$name" else name
    }

    /**
     * This returns a List of pairs which flatten fieldType into (class, name) pairs
     */
    def matchField(fieldType: Type, name: String): FieldBuilder =
      fieldType match {
        case tpe if tpe =:= typeOf[String] => Primitive(name, tpe)
        case tpe if tpe =:= typeOf[Boolean] => Primitive(name, tpe)
        case tpe if tpe =:= typeOf[Short] => Primitive(name, tpe)
        case tpe if tpe =:= typeOf[Int] => Primitive(name, tpe)
        case tpe if tpe =:= typeOf[Long] => Primitive(name, tpe)
        case tpe if tpe =:= typeOf[Float] => Primitive(name, tpe)
        case tpe if tpe =:= typeOf[Double] => Primitive(name, tpe)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          OptionBuilder(matchField(innerType, name))
        case tpe if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) =>
          CaseClassBuilder(name, expandMethod(tpe).map { case (t, s) => matchField(t, s) })
        case tpe if allowUnknownTypes => Primitive(name, tpe)
        case tpe =>
          c.abort(c.enclosingPosition, s"${T.tpe} is unsupported at $tpe")
      }

    def expandMethod(outerTpe: Type): Vector[(Type, String)] =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType.asSeenFrom(outerTpe, outerTpe.typeSymbol.asClass)
          (fieldType, fieldName)
        }.toVector

    val builder = matchField(T.tpe, "")
    if (builder.columnTypes.isEmpty)
      c.abort(c.enclosingPosition, s"Case class ${T.tpe} has no primitive types we were able to extract")
    val scheme = if (isNumbered(T.tpe)) Indexed else namingScheme
    val tree = FieldBuilder.toFieldsTree(builder, scheme)
    c.Expr[cascading.tuple.Fields](tree)
  }
}
