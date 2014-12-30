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
import scala.util.Random

import com.twitter.scalding._
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.bijection.macros.impl.IsCaseClassImpl
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */

object TupleConverterImpl {

  def caseClassTupleConverterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    case class Extractor(toTree: Tree)
    case class Builder(toTree: Tree = q"")

    implicit val builderLiftable = new Liftable[Builder] {
      def apply(b: Builder): Tree = b.toTree
    }

    implicit val extractorLiftable = new Liftable[Extractor] {
      def apply(b: Extractor): Tree = b.toTree
    }

    def matchField(outerTpe: Type, idx: Int, inOption: Boolean): (Int, Extractor, List[Builder]) = {
      def getPrimitive(accessor: Tree, boxedType: Type, box: Option[Tree]) = {
        val primitiveGetter = q"""${accessor}(${idx})"""
        if (inOption) {
          val cachedResult = newTermName(c.fresh(s"cacheVal"))
          val boxed = box.map{ b => q"""$b($primitiveGetter)""" }.getOrElse(primitiveGetter)

          val builder = q"""
          val $cachedResult: $boxedType = if(t.getObject($idx) == null) {
              null.asInstanceOf[$boxedType]
            } else {
              $boxed
            }
          """
          (idx + 1,
            Extractor(q"$cachedResult"),
            List(Builder(builder)))
        } else {
          (idx + 1, Extractor(primitiveGetter), List[Builder]())
        }
      }

      outerTpe match {
        case tpe if tpe =:= typeOf[String] => getPrimitive(q"t.getString", typeOf[java.lang.String], None)
        case tpe if tpe =:= typeOf[Boolean] => getPrimitive(q"t.getBoolean", typeOf[java.lang.Boolean], Some(q"_root_.java.lang.Boolean.valueOf"))
        case tpe if tpe =:= typeOf[Short] => getPrimitive(q"t.getShort", typeOf[java.lang.Short], Some(q"_root_.java.lang.Short.valueOf"))
        case tpe if tpe =:= typeOf[Int] => getPrimitive(q"t.getInteger", typeOf[java.lang.Integer], Some(q"_root_.java.lang.Integer.valueOf"))
        case tpe if tpe =:= typeOf[Long] => getPrimitive(q"t.getLong", typeOf[java.lang.Long], Some(q"_root_.java.lang.Long.valueOf"))
        case tpe if tpe =:= typeOf[Float] => getPrimitive(q"t.getFloat", typeOf[java.lang.Float], Some(q"_root_.java.lang.Float.valueOf"))
        case tpe if tpe =:= typeOf[Double] => getPrimitive(q"t.getDouble", typeOf[java.lang.Double], Some(q"_root_.java.lang.Double.valueOf"))
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && inOption =>
          c.abort(c.enclosingPosition, s"Nested options do not make sense being mapped onto a tuple fields in cascading.")

        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head

          val (newIdx, extractor, builders) = matchField(innerType, idx, true)

          val cachedResult = newTermName(c.fresh(s"opti"))
          val build = Builder(q"""
          val $cachedResult = if($extractor == null) {
              _root_.scala.Option.empty[$innerType]
            } else {
              _root_.scala.Some($extractor)
            }
            """)
          (newIdx, Extractor(q"""$cachedResult"""), builders :+ build)
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandCaseClass(tpe, idx, inOption)
        case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives, Option of a primitive nested case classes")
      }
    }

    def expandCaseClass(outerTpe: Type, parentIdx: Int, inOption: Boolean): (Int, Extractor, List[Builder]) = {
      val (idx, extractors, builders) = outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .foldLeft((parentIdx, List[Extractor](), List[Builder]())) {
          case ((idx, oldExtractors, oldBuilders), accessorMethod) =>
            val (newIdx, extractors, builders) = matchField(accessorMethod.returnType, idx, inOption)
            (newIdx, oldExtractors :+ extractors, oldBuilders ::: builders)
        }
      // We use the random long here since the idx isn't safe with nested case classes just containing more case classes
      // since they won't change the idx
      val cachedResult = newTermName(c.fresh(s"cacheVal"))

      val simpleBuilder = q"${outerTpe.typeSymbol.companionSymbol}(..$extractors)"
      val builder = if (inOption) {
        val tstOpt = extractors.map(e => q"$e == null").foldLeft(Option.empty[Tree]) {
          case (e, nxt) =>
            e match {
              case Some(t) => Some(q"$t || $nxt")
              case None => Some(nxt)
            }
        }
        tstOpt match {
          case Some(tst) =>
            q"""
              val $cachedResult: $outerTpe = if($tst) {
                null
              } else {
                $simpleBuilder
              }
            """
          case None => q"val $cachedResult = $simpleBuilder"
        }
      } else {
        q"val $cachedResult = $simpleBuilder"
      }
      (
        idx,
        Extractor(q"$cachedResult"),
        builders :+ Builder(builder))
    }

    val (finalIdx, extractor, builders) = expandCaseClass(T.tpe, 0, false)
    if (finalIdx == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")

    val res = q"""
    new _root_.com.twitter.scalding.TupleConverter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
     override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
        ..$builders
        $extractor
      }
      override val arity: scala.Int = ${finalIdx}
    }
    """

    c.Expr[TupleConverter[T]](res)
  }
}
