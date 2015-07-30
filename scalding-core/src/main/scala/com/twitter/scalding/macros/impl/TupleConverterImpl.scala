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
  def caseClassTupleConverterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] =
    caseClassTupleConverterCommonImpl(c, false)

  def caseClassTupleConverterWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] =
    caseClassTupleConverterCommonImpl(c, true)

  def caseClassTupleConverterCommonImpl[T](c: Context, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._

    val maybeHandlePrimitive: Option[Tree] = {
      /**
       * This returns the Tree to get a single primitive out of a TupleEntry
       */
      def innerLoop(tpe: Type, inOption: Boolean): Option[Tree] = {
        def getPrimitive(primitiveGetter: Tree, boxedType: Type, box: Option[Tree]): Some[Tree] = Some(
          if (inOption) {
            val boxed = box.map { b => q"""$b($primitiveGetter)""" }.getOrElse(primitiveGetter)
            q"""
            if(t.getObject(0) == null) {
              _root_.scala.None
            } else {
              _root_.scala.Some($boxed)
            }
          """
          } else {
            primitiveGetter
          })

        tpe match {
          case tpe if tpe =:= typeOf[String] =>
            getPrimitive(q"""t.getString(0)""", typeOf[java.lang.String], None)
          case tpe if tpe =:= typeOf[Boolean] =>
            getPrimitive(q"""t.getBoolean(0)""", typeOf[java.lang.Boolean], Some(q"_root_.java.lang.Boolean.valueOf"))
          case tpe if tpe =:= typeOf[Short] =>
            getPrimitive(q"""t.getShort(0)""", typeOf[java.lang.Short], Some(q"_root_.java.lang.Short.valueOf"))
          case tpe if tpe =:= typeOf[Int] =>
            getPrimitive(q"""t.getInteger(0)""", typeOf[java.lang.Integer], Some(q"_root_.java.lang.Integer.valueOf"))
          case tpe if tpe =:= typeOf[Long] =>
            getPrimitive(q"""t.getLong(0)""", typeOf[java.lang.Long], Some(q"_root_.java.lang.Long.valueOf"))
          case tpe if tpe =:= typeOf[Float] =>
            getPrimitive(q"""t.getFloat(0)""", typeOf[java.lang.Float], Some(q"_root_.java.lang.Float.valueOf"))
          case tpe if tpe =:= typeOf[Double] =>
            getPrimitive(q"""t.getDouble(0)""", typeOf[java.lang.Double], Some(q"_root_.java.lang.Double.valueOf"))

          case tpe if tpe.erasure =:= typeOf[Option[Any]] && inOption =>
            c.abort(c.enclosingPosition, s"Nested options do not make sense being mapped onto a tuple fields in cascading.")

          case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
            val innerType = tpe.asInstanceOf[TypeRefApi].args.head
            innerLoop(innerType, true)

          case tpe => None
        }
      }
      innerLoop(T.tpe, false).map { resTree =>
        q"""
      new _root_.com.twitter.scalding.TupleConverter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
       override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
          $resTree
        }
        override val arity: _root_.scala.Int = 1
      }
    """
      }
    }

    val res = maybeHandlePrimitive.getOrElse {
      if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
        c.abort(c.enclosingPosition,
          s"""|We cannot enforce ${T.tpe} is a case class, either it is not a case class or
              |this macro call is possibly enclosed in a class.
              |This will mean the macro is operating on a non-resolved type.
              |Issue when building Converter.""")

      // This holds a type and a variable name (toTree)
      case class Extractor(tpe: Type, toTree: Tree)
      // This holds the code to populate the variable name in an Extractor
      case class Builder(toTree: Tree = q"")

      implicit val builderLiftable = new Liftable[Builder] { def apply(b: Builder): Tree = b.toTree }
      implicit val extractorLiftable = new Liftable[Extractor] { def apply(b: Extractor): Tree = b.toTree }

      @annotation.tailrec
      def normalized(tpe: Type): Type = {
        val norm = tpe.normalize
        if (!(norm =:= tpe)) normalized(norm) else tpe
      }

      def matchField(outerTpe: Type, idx: Int, inOption: Boolean): (Int, Extractor, List[Builder]) = {
        /**
         * This returns a List, to match the return type of matchField, but that List has size 0 or 1
         * PS: would be great to have dependent or refinement types here.
         */
        def getPrimitive(primitiveGetter: Tree, boxedType: Type, box: Option[Tree]): (Int, Extractor, List[Builder]) =
          if (inOption) {
            val cachedResult = newTermName(c.fresh(s"cacheVal"))
            val boxed = box.map { b => q"""$b($primitiveGetter)""" }.getOrElse(primitiveGetter)

            val builder = q"""
          val $cachedResult: $boxedType = if(t.getObject($idx) == null) {
              null.asInstanceOf[$boxedType]
            } else {
              $boxed
            }
          """
            (idx + 1,
              Extractor(boxedType, q"$cachedResult"),
              List(Builder(builder)))
          } else {
            (idx + 1, Extractor(outerTpe, primitiveGetter), Nil)
          }

        outerTpe match {
          /*
           * First we handle primitives, which never recurse
           */
          case tpe if tpe =:= typeOf[String] =>
            getPrimitive(q"""t.getString(${idx})""", typeOf[java.lang.String], None)
          case tpe if tpe =:= typeOf[Boolean] =>
            getPrimitive(q"""t.getBoolean(${idx})""", typeOf[java.lang.Boolean], Some(q"_root_.java.lang.Boolean.valueOf"))
          case tpe if tpe =:= typeOf[Short] =>
            getPrimitive(q"""t.getShort(${idx})""", typeOf[java.lang.Short], Some(q"_root_.java.lang.Short.valueOf"))
          case tpe if tpe =:= typeOf[Int] =>
            getPrimitive(q"""t.getInteger(${idx})""", typeOf[java.lang.Integer], Some(q"_root_.java.lang.Integer.valueOf"))
          case tpe if tpe =:= typeOf[Long] =>
            getPrimitive(q"""t.getLong(${idx})""", typeOf[java.lang.Long], Some(q"_root_.java.lang.Long.valueOf"))
          case tpe if tpe =:= typeOf[Float] =>
            getPrimitive(q"""t.getFloat(${idx})""", typeOf[java.lang.Float], Some(q"_root_.java.lang.Float.valueOf"))
          case tpe if tpe =:= typeOf[Double] =>
            getPrimitive(q"""t.getDouble(${idx})""", typeOf[java.lang.Double], Some(q"_root_.java.lang.Double.valueOf"))
          case tpe if tpe.erasure =:= typeOf[Option[Any]] && inOption =>
            c.abort(c.enclosingPosition, s"Nested options do not make sense being mapped onto a tuple fields in cascading.")

          /*
           * Options require recursion on the inner type
           */
          case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
            val innerType = tpe.asInstanceOf[TypeRefApi].args.head

            val (newIdx, extractor, builders) = matchField(innerType, idx, true)

            val cachedResult = newTermName(c.fresh(s"opti"))
            /*
             * when have an Option of a primitive, we return the boxed java class
             * TODO: so here we handle converting that back into a scala type.
             * We could perhaps cast here, since we are unboxing just to box again
             */
            val extractorTypeVal: Tree = if (extractor.tpe =:= innerType)
              extractor.toTree
            else
              q"${innerType.typeSymbol.companionSymbol}.unbox($extractor)"

            val build = Builder(q"""
          val $cachedResult = if($extractor == null) {
              _root_.scala.None: _root_.scala.Option[$innerType]
            } else {
              _root_.scala.Some($extractorTypeVal)
            }
            """)
            (newIdx, Extractor(tpe, q"""$cachedResult"""), builders :+ build)

          case tpe if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) =>
            expandCaseClass(tpe, idx, inOption)
          case tpe if allowUnknownTypes =>
            getPrimitive(q"""t.getObject(${idx}).asInstanceOf[$tpe]""", tpe, None)
          case tpe =>
            c.abort(c.enclosingPosition,
              s"TT Case class ${T} is not pure primitives, Option of a primitive, nested case classes when looking at type ${tpe}")
        }
      }

      def expandCaseClass(outerTpe: Type, parentIdx: Int, inOption: Boolean): (Int, Extractor, List[Builder]) = {
        val (idx, extractors, builders) = outerTpe
          .declarations
          .collect { case m: MethodSymbol if m.isCaseAccessor => m }
          .foldLeft((parentIdx, List[Extractor](), List[Builder]())) {
            case ((idx, oldExtractors, oldBuilders), accessorMethod) =>
              val fieldType = accessorMethod.returnType.asSeenFrom(outerTpe, outerTpe.typeSymbol.asClass)

              val (newIdx, extractor, builders) = matchField(fieldType, idx, inOption)
              (newIdx, oldExtractors :+ extractor, oldBuilders ::: builders)
          }

        val cachedResult = newTermName(c.fresh(s"cacheVal"))
        val simpleBuilder = q"${outerTpe.typeSymbol.companionSymbol}(..$extractors)"
        val builder = if (inOption) {
          val tstOpt = extractors.map(e => q"$e == null")
            .foldLeft(Option.empty[Tree]) {
              // Since nested options are not allowed
              // if we are in an option, the current is None if any of the members are None
              case (Some(t), nxt) => Some(q"$t || $nxt")
              case (None, nxt) => Some(nxt)
            }

          tstOpt match {
            case Some(tst) =>
              q"""
              val $cachedResult: $outerTpe = if ($tst) {
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
        (idx,
          Extractor(outerTpe, q"$cachedResult"),
          builders :+ Builder(builder))
      }

      val (finalIdx, extractor, builders) = expandCaseClass(T.tpe, 0, false)
      if (finalIdx == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")

      q"""
    new _root_.com.twitter.scalding.TupleConverter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
     override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
        ..$builders
        $extractor
      }
      override val arity: _root_.scala.Int = $finalIdx
    }
    """
    }

    c.Expr[TupleConverter[T]](res)
  }
}
