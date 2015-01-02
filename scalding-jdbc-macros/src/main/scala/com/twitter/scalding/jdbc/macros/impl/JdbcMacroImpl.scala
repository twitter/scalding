package com.twitter.scalding.jdbc.macros.impl

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.util.{ Success, Failure }

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.jdbc.{ ColumnDefinition, ColumnDefinitionProvider }
import com.twitter.scalding.jdbc.macros._
import com.twitter.scalding.jdbc.macros.impl.handler._

// Simple wrapper to pass around the string name format of fields
private[impl] case class FieldName(toStr: String) {
  override def toString = toStr
}

object JDBCMacroImpl {

  // Takes a type and its companion objects apply method
  // based on the args it takes gives back out a field name to symbol
  private[this] def getDefaultArgs(c: Context)(tpe: c.Type): Map[String, c.Expr[String]] = {
    import c.universe._
    val classSym = tpe.typeSymbol
    val moduleSym = classSym.companionSymbol
    if (moduleSym == NoSymbol) {
      c.abort(c.enclosingPosition, s"No companion for case class ${tpe} available. Possibly a nested class? These do not work with this macro.")
    }
    val apply = moduleSym.typeSignature.declaration(newTermName("apply")).asMethod
    // can handle only default parameters from the first parameter list
    // because subsequent parameter lists might depend on previous parameters
    apply.paramss.head.map(_.asTerm).zipWithIndex.flatMap{
      case (p, i) =>
        if (!p.isParamWithDefault) None
        else {
          val getterName = newTermName("apply$default$" + (i + 1))
          Some(p.name.toString -> c.Expr(q"${moduleSym}.$getterName.toString"))
        }
    }.toMap
  }

  def caseClassJDBCPayloadImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[ColumnDefinitionProvider[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    // Field To JDBCColumn
    def matchField(oTpe: Type,
      fieldName: FieldName,
      defaultValOpt: Option[c.Expr[String]],
      annotationInfo: List[(Type, Option[Int])],
      nullable: Boolean): scala.util.Try[List[c.Expr[ColumnDefinition]]] = {
      oTpe match {
        // String handling
        case tpe if tpe =:= typeOf[String] => StringTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable)
        case tpe if tpe =:= typeOf[Short] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "SMALLINT")
        case tpe if tpe =:= typeOf[Int] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "INT")
        case tpe if tpe =:= typeOf[Long] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "BIGINT")
        case tpe if tpe =:= typeOf[Double] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "DOUBLE")
        case tpe if tpe =:= typeOf[Boolean] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "TINYINT")
        case tpe if tpe =:= typeOf[scala.math.BigInt] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "BIGINT")
        case tpe if tpe =:= typeOf[java.util.Date] => DateTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && nullable == true =>
          Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName} which contains a nested option. This is not supported by this macro."))

        case tpe if tpe.erasure =:= typeOf[Option[Any]] && nullable == false =>
          if (defaultValOpt.isDefined)
            Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, with a default value. Options cannot have default values"))
          else {
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, fieldName, None, annotationInfo, true)
          }
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => Success(expandMethod(tpe, s"${fieldName}.").toList)

        // default
        case _ => Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, which is not supported for talking to JDBC"))
      }
    }

    def expandMethod(outerTpe: Type, fieldNamePrefix: String): Iterable[c.Expr[ColumnDefinition]] = {
      val defaultArgs = getDefaultArgs(c)(outerTpe)
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { m =>
          val fieldName = m.name.toTermName.toString
          val defaultVal = defaultArgs.get(fieldName)
          val annotationInfo: List[(Type, Option[Int])] = m.annotations
            .map(t => (t.tpe, t.scalaArgs))
            .collect {
              case (tpe, List(Literal(Constant(siz: Int)))) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.size] => (tpe, Some(siz))
              case (tpe, _) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.size] => c.abort(c.enclosingPosition, "Hit a size macro where we couldn't parse the value. Probably not a literal constant. Only literal constants are supported.")
              case (tpe, _) if tpe <:< typeOf[com.twitter.scalding.jdbc.macros.ScaldingJdbcAnnotation] => (tpe, None)
            }
          (m, fieldName, defaultVal, annotationInfo)
        }
        .flatMap {
          case (accessorMethod, fieldName, defaultVal, annotationInfo) =>
            matchField(accessorMethod.returnType, FieldName(fieldNamePrefix + fieldName), defaultVal, annotationInfo, false) match {
              case Success(s) => s
              case Failure(e) => (c.abort(c.enclosingPosition, e.getMessage))
            }
        }
    }

    val columns = expandMethod(T.tpe, "")
    val res = q"""
    new _root_.com.twitter.scalding.jdbc.ColumnDefinitionProvider[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override val columns = List(..$columns)
    }
    """
    c.Expr[ColumnDefinitionProvider[T]](res)
  }
}
