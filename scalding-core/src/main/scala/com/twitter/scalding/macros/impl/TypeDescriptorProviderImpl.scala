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
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object TypeDescriptorProviderImpl {

  def caseClassTypeDescriptorImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TypeDescriptor[T]] =
    caseClassTypeDescriptorCommonImpl(c, false)(T)

  def caseClassTypeDescriptorWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TypeDescriptor[T]] =
    caseClassTypeDescriptorCommonImpl(c, true)(T)

  /**
   * When flattening a nested structure with Options, the evidentColumn is a column, relative to the
   * the first 0-offset column, that represents evidence of this T, and hence set of columns, are
   * present or absent. This is to handle Option types in text files such as CSV and TSV.
   * a type T is evident if it the evidentColumn.exists
   *
   * primitive numbers are evident
   * case classes are evident if they have at least one evident member.
   *
   * Strings are not evident (we can't distinguish Empty from "")
   * Option[T] is not evident (we can't tell Some(None) from None).
   */
  def evidentColumn(c: Context, allowUnknown: Boolean = false)(tpe: c.universe.Type): Option[Int] = {
    import c.universe._

    def flattenOnce(t: Type): List[Type] =
      t.declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map(_.returnType.asSeenFrom(t, t.typeSymbol.asClass))
        .toList

    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    def go(t: Type, offset: Int): (Int, Option[Int]) = {
      val thisColumn = (offset + 1, Some(offset))
      t match {
        case tpe if tpe =:= typeOf[String] =>
          // if we don't allowUnknown here, we treat null and "" is indistinguishable
          // for text formats
          if (allowUnknown) thisColumn
          else (offset + 1, None)
        case tpe if tpe =:= typeOf[Boolean] => thisColumn
        case tpe if tpe =:= typeOf[Short] => thisColumn
        case tpe if tpe =:= typeOf[Int] => thisColumn
        case tpe if tpe =:= typeOf[Long] => thisColumn
        case tpe if tpe =:= typeOf[Float] => thisColumn
        case tpe if tpe =:= typeOf[Double] => thisColumn
        // We recurse on Option and case classes
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerTpe = optionInner(c)(tpe).get
          // we have no evidentColumn, but we need to compute the next index
          (go(innerTpe, offset)._1, None)
        case tpe if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) =>
          val flattened = flattenOnce(tpe)
            .scanLeft((offset, Option.empty[Int])) { case ((off, _), t) => go(t, off) }

          val nextPos = flattened.last._1
          val ev = flattened.collectFirst { case (_, Some(col)) => col }
          (nextPos, ev)
        case _ if allowUnknown => thisColumn
        case t =>
          c.abort(c.enclosingPosition, s"Case class ${tpe} at $t is not pure primitives or nested case classes")
      }
    }
    go(tpe, 0)._2
  }

  def optionInner(c: Context)(opt: c.universe.Type): Option[c.universe.Type] =
    if (opt.erasure =:= c.universe.typeOf[Option[Any]]) {
      Some(opt.asInstanceOf[c.universe.TypeRefApi].args.head)
    } else None

  def isTuple[T](c: Context)(implicit T: c.WeakTypeTag[T]): Boolean = {
    import c.universe._
    val tupleTypes = List(typeOf[Tuple1[Any]],
      typeOf[Tuple2[Any, Any]],
      typeOf[Tuple3[Any, Any, Any]],
      typeOf[Tuple4[Any, Any, Any, Any]],
      typeOf[Tuple5[Any, Any, Any, Any, Any]],
      typeOf[Tuple6[Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple7[Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
      typeOf[Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]])
    (tupleTypes.exists { _ =:= T.tpe.erasure })
  }

  def caseClassTypeDescriptorCommonImpl[T](c: Context, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[TypeDescriptor[T]] = {
    import c.universe._

    val converter = TupleConverterImpl.caseClassTupleConverterCommonImpl[T](c, allowUnknownTypes)
    val setter = TupleSetterImpl.caseClassTupleSetterCommonImpl[T](c, allowUnknownTypes)

    val namingScheme = if (isTuple[T](c)) Indexed else NamedWithPrefix

    val fields = FieldsProviderImpl.toFieldsCommonImpl[T](c, namingScheme, allowUnknownTypes)

    val res = q"""
    new _root_.com.twitter.scalding.TypeDescriptor[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override val converter = $converter
      override val setter = $setter
      override val fields = $fields
    }
    """
    c.Expr[TypeDescriptor[T]](res)
  }

}
