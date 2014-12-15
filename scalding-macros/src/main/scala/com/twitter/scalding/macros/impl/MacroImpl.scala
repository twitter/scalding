package com.twitter.scalding.macros.impl

import scala.collection.mutable.{ Map => MMap }
import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Try => BasicTry }

import cascading.tuple.{ Tuple, TupleEntry }

import com.twitter.scalding._
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.bijection.macros.impl.IsCaseClassImpl
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object MacroImpl {
  def caseClassTupleSetterNoProof[T]: TupleSetter[T] = macro caseClassTupleSetterNoProofImpl[T]

  def caseClassTupleSetterImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] =
    caseClassTupleSetterNoProofImpl(c)(T)

  def caseClassTupleSetterNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._
    //TODO get rid of the mutability
    val cachedTupleSetters: MMap[Type, Int] = MMap.empty
    var cacheIdx = 0

    def expandMethod(mSeq: Iterable[MethodSymbol], pTree: Tree): Iterable[Int => Tree] = {
      mSeq.flatMap { accessorMethod =>
        accessorMethod.returnType match {
          case tpe if tpe =:= typeOf[String] => List((idx: Int) => q"""tup.setString(${idx}, $pTree.$accessorMethod)""")
          case tpe if tpe =:= typeOf[Boolean] => List((idx: Int) => q"""tup.setBoolean(${idx}, $pTree.$accessorMethod)""")
          case tpe if tpe =:= typeOf[Short] => List((idx: Int) => q"""tup.setShort(${idx}, $pTree.$accessorMethod)""")
          case tpe if tpe =:= typeOf[Int] => List((idx: Int) => q"""tup.setInteger(${idx}, $pTree.$accessorMethod)""")
          case tpe if tpe =:= typeOf[Long] => List((idx: Int) => q"""tup.setLong(${idx}, $pTree.$accessorMethod)""")
          case tpe if tpe =:= typeOf[Float] => List((idx: Int) => q"""tup.setFloat(${idx}, $pTree.$accessorMethod)""")
          case tpe if tpe =:= typeOf[Double] => List((idx: Int) => q"""tup.setDouble(${idx}, $pTree.$accessorMethod)""")
          case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
            expandMethod(tpe.declarations
              .collect { case m: MethodSymbol if m.isCaseAccessor => m },
              q"""$pTree.$accessorMethod""")
          case _ => List((idx: Int) => q"""tup.set(${idx}, t.$accessorMethod)""")
        }
      }
    }

    val set =
      expandMethod(T.tpe.declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }, q"t")
        .zipWithIndex
        .map {
          case (treeGenerator, idx) =>
            treeGenerator(idx)
        }

    val res = q"""
    new _root_.com.twitter.scalding.TupleSetter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override def apply(t: $T): _root_.cascading.tuple.Tuple = {
        val tup = _root_.cascading.tuple.Tuple.size(${set.size})
        ..$set
        tup
      }
      override val arity: scala.Int = ${set.size}
    }
    """
    c.Expr[TupleSetter[T]](res)
  }

  def caseClassTupleConverterNoProof[T]: TupleConverter[T] = macro caseClassTupleConverterNoProofImpl[T]

  def caseClassTupleConverterImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] =
    caseClassTupleConverterNoProofImpl(c)(T)

  def caseClassTupleConverterNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._
    //TODO get rid of the mutability
    val cachedTupleConverters: MMap[Type, Int] = MMap.empty
    var cacheIdx = 0
    case class AccessorBuilder(builder: Tree, size: Int)

    def getPrimitive(strAccessor: Tree): Int => AccessorBuilder =
      { (idx: Int) =>
        AccessorBuilder(q"""${strAccessor}(${idx})""", 1)
      }

    def flattenAccessorBuilders(tpe: Type, idx: Int, childGetters: List[(Int => AccessorBuilder)]): AccessorBuilder = {
      val (_, accessors) = childGetters.foldLeft((idx, List[AccessorBuilder]())) {
        case ((curIdx, eles), t) =>
          val nextEle = t(curIdx)
          val idxIncr = nextEle.size
          (curIdx + idxIncr, eles :+ nextEle)
      }

      val builder = q"""
        ${tpe.typeSymbol.companionSymbol}(..${accessors.map(_.builder)})
      """
      val size = accessors.map(_.size).reduce(_ + _)
      AccessorBuilder(builder, size)
    }

    def expandMethod(outerTpe: Type): List[(Int => AccessorBuilder)] = {
      outerTpe.declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m.returnType }
        .toList
        .map { accessorMethod =>
          accessorMethod match {
            case tpe if tpe =:= typeOf[String] => getPrimitive(q"t.getString")
            case tpe if tpe =:= typeOf[Boolean] => getPrimitive(q"t.Boolean")
            case tpe if tpe =:= typeOf[Short] => getPrimitive(q"t.getShort")
            case tpe if tpe =:= typeOf[Int] => getPrimitive(q"t.getInteger")
            case tpe if tpe =:= typeOf[Long] => getPrimitive(q"t.getLong")
            case tpe if tpe =:= typeOf[Float] => getPrimitive(q"t.getFloat")
            case tpe if tpe =:= typeOf[Double] => getPrimitive(q"t.getDouble")
            case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
              { (idx: Int) =>
                {
                  val childGetters = expandMethod(tpe)
                  flattenAccessorBuilders(tpe, idx, childGetters)
                }
              }
            case tpe =>
              ((idx: Int) =>
                AccessorBuilder(q"""t.getObject(${idx}).asInstanceOf[$tpe]""", 1))
          }
        }
    }

    val accessorBuilders = flattenAccessorBuilders(T.tpe, 0, expandMethod(T.tpe))

    val res = q"""
    new _root_.com.twitter.scalding.TupleConverter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
     override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
        ${accessorBuilders.builder}
      }
      override val arity: scala.Int = ${accessorBuilders.size}
    }
    """
    c.Expr[TupleConverter[T]](res)
  }
}
