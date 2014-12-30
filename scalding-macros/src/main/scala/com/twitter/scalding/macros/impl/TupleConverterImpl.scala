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

  def caseClassTupleConverterNoProof[T]: TupleConverter[T] = macro caseClassTupleConverterNoProofImpl[T]

  def caseClassTupleConverterImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] =
    caseClassTupleConverterNoProofImpl(c)(T)

  def caseClassTupleConverterNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._
    case class Extractor(toTree: Tree)
    case class Builder(toTree: Tree = q"")

    implicit val builderLiftable = new Liftable[Builder] {
      def apply(b: Builder): Tree = b.toTree
    }

    implicit val extractorLiftable = new Liftable[Extractor] {
      def apply(b: Extractor): Tree = b.toTree
    }

    def matchField(outerTpe: Type, idx: Int, inOption: Boolean): (Int, Extractor, List[Builder]) = {
      def getPrimitive(accessor: Tree, box: Option[Tree]) = {
        val primitiveGetter = q"""${accessor}(${idx})"""
        if (inOption) {
          val cachedResult = newTermName(c.fresh(s"cacheVal"))
          val boxed = box.map{ b => q"""$b($primitiveGetter)""" }.getOrElse(primitiveGetter)

          val builder = q"""
          val $cachedResult: $outerTpe = if(t.getObject($idx) == null) {
              null.asInstanceOf[$outerTpe]
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
        case tpe if tpe =:= typeOf[String] => getPrimitive(q"t.getString", None)
        case tpe if tpe =:= typeOf[Boolean] => getPrimitive(q"t.Boolean", Some(q"_root_.scala.Boolean.box"))
        case tpe if tpe =:= typeOf[Short] => getPrimitive(q"t.getShort", Some(q"_root_.scala.Short.box"))
        case tpe if tpe =:= typeOf[Int] => getPrimitive(q"t.getInteger", Some(q"_root_.scala.Int.box"))
        case tpe if tpe =:= typeOf[Long] => getPrimitive(q"t.getLong", Some(q"_root_.scala.Long.box"))
        case tpe if tpe =:= typeOf[Float] => getPrimitive(q"t.getFloat", Some(q"_root_.scala.Float.box"))
        case tpe if tpe =:= typeOf[Double] => getPrimitive(q"t.getDouble", Some(q"_root_.scala.Double.box"))
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
