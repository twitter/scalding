package com.twitter.scalding.jdbc.macros.impl

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Success, Failure }

import com.twitter.scalding._
import com.twitter.bijection.macros.IsCaseClass
import com.twitter.scalding.jdbc.macros._

/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object JdbcMacroImpl {
  def caseClassJdbcPayloadNoProof[T]: JDBCDescriptor[T] = macro caseClassJdbcPayloadNoProofImpl[T]

  def caseClassJdbcPayloadImpl[T](c: Context)(proof: c.Expr[IsCaseClass[T]])(implicit T: c.WeakTypeTag[T]): c.Expr[JDBCDescriptor[T]] =
    caseClassJdbcPayloadNoProofImpl(c)(T)

  // Takes a type and its companion objects apply method
  // based on the args it takes gives back out a field name to symbol
  private[this] def getDefaultArgs[T](c: Context)(T: c.WeakTypeTag[T]): Map[String, c.universe.Select] = {
    import c.universe._
    val classSym = T.tpe.typeSymbol
    val moduleSym = classSym.companionSymbol
    val apply = moduleSym.typeSignature.declaration(newTermName("apply")).asMethod
    // can handle only default parameters from the first parameter list
    // because subsequent parameter lists might depend on previous parameters
    apply.paramss.head.map(_.asTerm).zipWithIndex.flatMap{
      case (p, i) =>
        if (!p.isParamWithDefault) None
        else {
          val getterName = newTermName("apply$default$" + (i + 1))
          Some(p.name.toString -> q"${moduleSym}.$getterName")
        }
    }.toMap
  }

  def caseClassJdbcPayloadNoProofImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[JDBCDescriptor[T]] = {
    import c.universe._

    val defaultArgs = getDefaultArgs[T](c)(T)

    // Field To JdbcColumn
    def matchField(oTpe: Type, fieldName: String, defaultValOpt: Option[Select], sizeOpt: Option[Int]): scala.util.Try[Tree] =
      (oTpe, sizeOpt) match {
        // String handling
        case (tpe, Some(size)) if tpe =:= typeOf[String] => Success(q"StringColumn($fieldName, $size, $defaultValOpt)")
        case (tpe, None) if tpe =:= typeOf[String] => Failure(new Exception(s"Case class ${T} has a string field without annotation"))

        // Int handling
        case (tpe, Some(size)) if tpe =:= typeOf[Int] => Success(q"IntColumn($fieldName, $size, $defaultValOpt)")
        case (tpe, None) if tpe =:= typeOf[Int] => Success(q"IntColumn($fieldName, 32, $defaultValOpt)")
        case (tpe, s) if tpe.erasure =:= typeOf[Option[Any]] =>
          if (defaultValOpt.isDefined)
            Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, with a default value. Options cannot have default values"))
          else
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, fieldName, None, sizeOpt).map(expr => q"OptionalColumn($expr)")

        // default
        case _ => Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, which is not supported for talking to jdbc"))
      }

    def expandMethod(outerTpe: Type, pTree: Tree): Iterable[Tree] = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { m =>
          val fieldName = m.name.toTermName.toString
          val defaultVal = defaultArgs.get(fieldName)
          val optiSizeAnnotation: Option[Int] = m.annotations
            .map(t => (t.tpe, t.scalaArgs))
            .collect { case (tpe, List(Literal(Constant(siz: Int)))) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.size] => siz }
            .headOption

          (m, fieldName, defaultVal, optiSizeAnnotation)
        }
        .map {
          case (accessorMethod, fieldName, defaultVal, sizeOpt) =>
            matchField(accessorMethod.returnType, fieldName, defaultVal, sizeOpt) match {
              case Success(s) => s
              case Failure(e) => (c.abort(c.enclosingPosition, e.getMessage))
            }
        }
    }

    val columns = expandMethod(T.tpe, q"t")

    val res = q"""
    new _root_.com.twitter.scalding.jdbc.macros.JDBCDescriptor[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override val columns = List(..$columns)
    }
    """
    c.Expr[JDBCDescriptor[T]](res)
  }
}
