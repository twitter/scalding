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

    def expandMethod(outerTpe: Type, pTree: Tree): Iterable[Int => Tree] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .flatMap { accessorMethod =>
          accessorMethod.returnType match {
            case tpe if tpe =:= typeOf[String] => List((idx: Int) => q"""tup.setString(${idx}, $pTree.$accessorMethod)""")
            case tpe if tpe =:= typeOf[Boolean] => List((idx: Int) => q"""tup.setBoolean(${idx}, $pTree.$accessorMethod)""")
            case tpe if tpe =:= typeOf[Short] => List((idx: Int) => q"""tup.setShort(${idx}, $pTree.$accessorMethod)""")
            case tpe if tpe =:= typeOf[Int] => List((idx: Int) => q"""tup.setInteger(${idx}, $pTree.$accessorMethod)""")
            case tpe if tpe =:= typeOf[Long] => List((idx: Int) => q"""tup.setLong(${idx}, $pTree.$accessorMethod)""")
            case tpe if tpe =:= typeOf[Float] => List((idx: Int) => q"""tup.setFloat(${idx}, $pTree.$accessorMethod)""")
            case tpe if tpe =:= typeOf[Double] => List((idx: Int) => q"""tup.setDouble(${idx}, $pTree.$accessorMethod)""")
            case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
              expandMethod(tpe,
                q"""$pTree.$accessorMethod""")
            case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives or nested case classes")
          }
        }
    }

    val set =
      expandMethod(T.tpe, q"t")
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

  def toFieldsImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] =
    toFieldsNoProofImpl(c)(T)

  def toFieldsNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[cascading.tuple.Fields] = {
    import c.universe._
    //TODO get rid of the mutability
    val cachedTupleSetters: MMap[Type, Int] = MMap.empty
    var cacheIdx = 0

    def expandMethod(outerTpe: Type, outerName: String): List[(Tree, String)] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .flatMap { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          val simpleRet = List((q"""classOf[$fieldType]""", s"$outerName$fieldName"))
          accessorMethod.returnType match {
            case tpe if tpe =:= typeOf[String] => simpleRet
            case tpe if tpe =:= typeOf[Boolean] => simpleRet
            case tpe if tpe =:= typeOf[Short] => simpleRet
            case tpe if tpe =:= typeOf[Int] => simpleRet
            case tpe if tpe =:= typeOf[Long] => simpleRet
            case tpe if tpe =:= typeOf[Float] => simpleRet
            case tpe if tpe =:= typeOf[Double] => simpleRet
            case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(tpe, s"$outerName$fieldName.")
            case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives or nested case classes")
          }
        }.toList
    }

    val expanded = expandMethod(T.tpe, "")
    val typeTrees = expanded.map(_._1)
    val fieldNames = expanded.map(_._2)

    val res = q"""
      new cascading.tuple.Fields(scala.Array.apply[java.lang.Comparable[_]](..$fieldNames), scala.Array.apply[java.lang.reflect.Type](..$typeTrees))
      """
    c.Expr[cascading.tuple.Fields](res)
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
            case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives or nested case classes")
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
