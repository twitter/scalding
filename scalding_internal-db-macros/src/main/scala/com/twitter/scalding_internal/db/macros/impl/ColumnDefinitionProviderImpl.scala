package com.twitter.scalding_internal.db.macros.impl

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.util.{ Success, Failure }

import com.twitter.scalding_internal.db.macros.impl.upstream.bijection.IsCaseClassImpl
import com.twitter.scalding_internal.db.{ ColumnDefinition, ColumnDefinitionProvider, ResultSetExtractor }
import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding_internal.db.macros.impl.handler._

// Simple wrapper to pass around the string name format of fields
private[impl] case class FieldName(toStr: String) {
  override def toString = toStr
}

object ColumnDefinitionProviderImpl {

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

  private def getColumnFormats[T](c: Context)(implicit T: c.WeakTypeTag[T]): List[ColumnFormat[c.type]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    // Field To JDBCColumn
    def matchField(oTpe: Type,
      fieldName: FieldName,
      defaultValOpt: Option[c.Expr[String]],
      annotationInfo: List[(Type, Option[Int])],
      nullable: Boolean): scala.util.Try[List[ColumnFormat[c.type]]] = {
      oTpe match {
        // String handling
        case tpe if tpe =:= typeOf[String] => StringTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable)
        case tpe if tpe =:= typeOf[Short] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "SMALLINT")
        case tpe if tpe =:= typeOf[Int] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "INT")
        case tpe if tpe =:= typeOf[Long] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "BIGINT")
        case tpe if tpe =:= typeOf[Double] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "DOUBLE")
        case tpe if tpe =:= typeOf[Boolean] => NumericTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable, "BOOLEAN")
        case tpe if tpe =:= typeOf[java.util.Date] => DateTypeHandler(c)(fieldName, defaultValOpt, annotationInfo, nullable)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && nullable == true =>
          Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName} which contains a nested option. This is not supported by this macro."))

        case tpe if tpe.erasure =:= typeOf[Option[Any]] && nullable == false =>
          if (defaultValOpt.isDefined)
            Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, with a default value. Options cannot have default values"))
          else {
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, fieldName, None, annotationInfo, true)
          }
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(tpe, s"${fieldName}.")

        // default
        case _ => Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, which is not supported for talking to JDBC"))
      }
    }

    def expandMethod(outerTpe: Type, fieldNamePrefix: String): scala.util.Try[List[ColumnFormat[c.type]]] = {
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
              case (tpe, List(Literal(Constant(siz: Int)))) if tpe =:= typeOf[com.twitter.scalding_internal.db.macros.size] => (tpe, Some(siz))
              case (tpe, _) if tpe =:= typeOf[com.twitter.scalding_internal.db.macros.size] => c.abort(c.enclosingPosition, "Hit a size macro where we couldn't parse the value. Probably not a literal constant. Only literal constants are supported.")
              case (tpe, _) if tpe <:< typeOf[com.twitter.scalding_internal.db.macros.ScaldingDBAnnotation] => (tpe, None)
            }
          (m, fieldName, defaultVal, annotationInfo)
        }
        .map {
          case (accessorMethod, fieldName, defaultVal, annotationInfo) =>
            matchField(accessorMethod.returnType, FieldName(fieldNamePrefix + fieldName), defaultVal, annotationInfo, false)
        }
        .toList
        // This algorithm returns the error from the first exception we run into.
        .foldLeft(scala.util.Try[List[ColumnFormat[c.type]]](Nil)) {
          case (pTry, nxt) =>
            (pTry, nxt) match {
              case (Success(l), Success(r)) => Success(l ::: r)
              case (f @ Failure(_), _) => f
              case (_, f @ Failure(_)) => f
            }
        }
    }

    expandMethod(T.tpe, "") match {
      case Success(s) => s
      case Failure(e) => (c.abort(c.enclosingPosition, e.getMessage))
    }
  }

  def getColumnDefn[T](c: Context)(implicit T: c.WeakTypeTag[T]): List[c.Expr[ColumnDefinition]] = {
    import c.universe._

    val columnFormats = getColumnFormats[T](c)

    columnFormats.map {
      case cf: ColumnFormat[_] =>
        val nullableVal = if (cf.nullable)
          q"_root_.com.twitter.scalding_internal.db.Nullable"
        else
          q"_root_.com.twitter.scalding_internal.db.NotNullable"
        val fieldTypeSelect = Select(q"_root_.com.twitter.scalding_internal.db", newTermName(cf.fieldType))
        val res = q"""new _root_.com.twitter.scalding_internal.db.ColumnDefinition(
        $fieldTypeSelect,
        _root_.com.twitter.scalding_internal.db.ColumnName(${cf.fieldName.toStr}),
        $nullableVal,
        ${cf.sizeOpt},
        ${cf.defaultValue})
        """
        c.Expr[ColumnDefinition](res)
    }
  }

  def getExtractor[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[ResultSetExtractor[T]] = {
    import c.universe._

    val columnFormats = getColumnFormats[T](c)

    val rsmdTerm = newTermName(c.fresh("rsmd"))
    // we validate two things from ResultSetMetadata
    // 1. the column types match with actual DB schema
    // 2. all non-nullable fields are indeed non-nullable in DB schema
    val checks = columnFormats.zipWithIndex.map {
      case (cf: ColumnFormat[_], pos: Int) =>
        val fieldName = cf.fieldName.toStr
        val typeNameTerm = newTermName(c.fresh(s"colTypeName_$pos"))
        val typeName = q"""
        val $typeNameTerm = $rsmdTerm.getColumnTypeName(${pos + 1})
        """
        // certain types have synonyms, so we group them together here
        // note: this is mysql specific
        // http://dev.mysql.com/doc/refman/5.0/en/numeric-type-overview.html
        val typeValidation = cf.fieldType match {
          case "VARCHAR" => q"""List("VARCHAR", "CHAR").contains($typeNameTerm)"""
          case "BOOLEAN" | "TINYINT" => q"""List("BOOLEAN", "BOOL", "TINYINT").contains($typeNameTerm)"""
          case "INT" => q"""List("INTEGER", "INT").contains($typeNameTerm)"""
          case f => q"""$f == $typeNameTerm"""
        }
        val typeAssert = q"""
        if (!$typeValidation) {
          throw new _root_.com.twitter.scalding_internal.db.JdbcValidationException(
            "Mismatched type for column '" + $fieldName + "'. Expected " + ${cf.fieldType} +
              " but set to " + $typeNameTerm + " in DB.")
        }
        """
        val nullableTerm = newTermName(c.fresh(s"isNullable_$pos"))
        val nullableValidation = q"""
        val $nullableTerm = $rsmdTerm.isNullable(${pos + 1})
        if ($nullableTerm == _root_.java.sql.ResultSetMetaData.columnNoNulls && ${cf.nullable}) {
          throw new _root_.com.twitter.scalding_internal.db.JdbcValidationException(
            "Column '" + $fieldName + "' is not nullable in DB.")
        }
        """
        q"""
        $typeName
        $typeAssert
        $nullableValidation
        """
    }

    val rsTerm = newTermName(c.fresh("rs"))
    val formats = columnFormats.map {
      case cf: ColumnFormat[_] => {
        val fieldName = cf.fieldName.toStr
        // java boxed types needed below to populate cascading's Tuple
        cf.fieldType match {
          case "VARCHAR" | "TEXT" => q"""$rsTerm.getString($fieldName)"""
          case "BOOLEAN" | "TINYINT" => q"""_root_.java.lang.Boolean.valueOf($rsTerm.getBoolean($fieldName))"""
          case "DATE" | "DATETIME" => q"""Option($rsTerm.getTimestamp($fieldName)).map { ts => new java.util.Date(ts.getTime) }.orNull"""
          // dates set to null are populated as None by tuple converter
          // if the corresponding case class field is an Option[Date]
          case "DOUBLE" => q"""_root_.java.lang.Double.valueOf($rsTerm.getDouble($fieldName))"""
          case "BIGINT" => q"""_root_.java.lang.Long.valueOf($rsTerm.getLong($fieldName))"""
          case "INT" | "SMALLINT" => q"""_root_.java.lang.Integer.valueOf($rsTerm.getInt($fieldName))"""
          case f => q"""sys.error("Invalid format " + $f + " for " + $fieldName)"""
        }
        // note: UNSIGNED BIGINT is currently unsupported
      }
    }
    val tcTerm = newTermName(c.fresh("conv"))
    val res = q"""
    new _root_.com.twitter.scalding_internal.db.ResultSetExtractor[$T] {
      def validate($rsmdTerm: _root_.java.sql.ResultSetMetaData): _root_.scala.util.Try[Unit] = _root_.scala.util.Try { ..$checks }
      def toCaseClass($rsTerm: java.sql.ResultSet, $tcTerm: _root_.com.twitter.scalding.TupleConverter[$T]): $T =
        $tcTerm(new _root_.cascading.tuple.TupleEntry(new _root_.cascading.tuple.Tuple(..$formats)))
    }
    """
    // ResultSet -> TupleEntry -> case class
    c.Expr[ResultSetExtractor[T]](res)
  }

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[ColumnDefinitionProvider[T]] = {
    import c.universe._

    val columns = getColumnDefn[T](c)
    val resultSetExtractor = getExtractor[T](c)

    val res = q"""
    new _root_.com.twitter.scalding_internal.db.ColumnDefinitionProvider[$T] with _root_.com.twitter.scalding_internal.db.macros.upstream.bijection.MacroGenerated {
      override val columns = List(..$columns)
      override val resultSetExtractor = $resultSetExtractor
    }
    """
    c.Expr[ColumnDefinitionProvider[T]](res)
  }
}
