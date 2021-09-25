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

import com.twitter.scalding._
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */

object TupleConverterImpl {
  def caseClassTupleConverterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] =
    caseClassTupleConverterCommonImpl(c, false)

  def caseClassTupleConverterWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] =
    caseClassTupleConverterCommonImpl(c, true)

  def caseClassTupleConverterCommonImpl[T](c: Context, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._

    import TypeDescriptorProviderImpl.evidentColumn

    def membersOf(outerTpe: Type): Vector[Type] =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { accessorMethod =>
          accessorMethod.returnType.asSeenFrom(outerTpe, outerTpe.typeSymbol.asClass)
        }
        .toVector

    sealed trait ConverterBuilder {
      def columns: Int
      def applyTree(offset: Int): Tree
    }
    final case class PrimitiveBuilder(primitiveGetter: Int => Tree) extends ConverterBuilder {
      def columns = 1
      def applyTree(offset: Int) = primitiveGetter(offset)
    }
    final case class OptionBuilder(evidentCol: Int, of: ConverterBuilder) extends ConverterBuilder {
      def columns = of.columns
      def applyTree(offset: Int) = {
        val testIdx = offset + evidentCol
        q"""if (t.getObject($testIdx) == null) None
            else Some(${of.applyTree(offset)})"""
      }
    }
    final case class CaseClassBuilder(tpe: Type, members: Vector[ConverterBuilder]) extends ConverterBuilder {
      val columns = members.map(_.columns).sum
      def applyTree(offset: Int) = {
        val trees = members.scanLeft((offset, Option.empty[Tree])) {
          case ((o, _), cb) =>
            val nextOffset = o + cb.columns
            (nextOffset, Some(cb.applyTree(o)))
        }
          .collect { case (_, Some(tree)) => tree }

        q"${tpe.typeSymbol.companionSymbol}(..$trees)"
      }
    }

    def matchField(outerTpe: Type): ConverterBuilder =
      outerTpe match {
        /*
         * First we handle primitives, which never recurse
         */
        case tpe if tpe =:= typeOf[String] && allowUnknownTypes =>
          PrimitiveBuilder(idx => q"""t.getString($idx)""")
        case tpe if tpe =:= typeOf[String] =>
          // In this case, null is identical to empty, and we always return non-null
          PrimitiveBuilder(idx => q"""{val s = t.getString($idx); if (s == null) "" else s}""")
        case tpe if tpe =:= typeOf[Boolean] =>
          PrimitiveBuilder(idx => q"""t.getBoolean($idx)""")
        case tpe if tpe =:= typeOf[Short] =>
          PrimitiveBuilder(idx => q"""t.getShort($idx)""")
        case tpe if tpe =:= typeOf[Int] =>
          PrimitiveBuilder(idx => q"""t.getInteger($idx)""")
        case tpe if tpe =:= typeOf[Long] =>
          PrimitiveBuilder(idx => q"""t.getLong($idx)""")
        case tpe if tpe =:= typeOf[Float] =>
          PrimitiveBuilder(idx => q"""t.getFloat($idx)""")
        case tpe if tpe =:= typeOf[Double] =>
          PrimitiveBuilder(idx => q"""t.getDouble($idx)""")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          evidentColumn(c, allowUnknownTypes)(innerType) match {
            case None => // there is no evident column, not supported.
              c.abort(c.enclosingPosition, s"$tpe has unsupported nesting of Options at: $innerType")
            case Some(ev) => // we can recurse here
              OptionBuilder(ev, matchField(innerType))
          }
        case tpe if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) =>
          CaseClassBuilder(tpe, membersOf(tpe).map(matchField))
        case tpe if allowUnknownTypes =>
          PrimitiveBuilder(idx => q"""t.getObject(${idx}).asInstanceOf[$tpe]""")
        case tpe =>
          c.abort(c.enclosingPosition,
            s"${T.tpe} is not pure primitives, Option of a primitive, nested case classes when looking at type ${tpe}")
      }

    val builder = matchField(T.tpe)
    if (builder.columns == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")

    val res = q"""
  new _root_.com.twitter.scalding.TupleConverter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
   override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
      ${builder.applyTree(0)}
    }
    override val arity: _root_.scala.Int = ${builder.columns}
  }
  """
    c.Expr[TupleConverter[T]](res)
  }
}
