package com.twitter.scalding.db.macros.impl

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.util.{ Success, Failure }

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.db.{ ColumnDefinition, ColumnDefinitionProvider, ResultSetExtractor }
import com.twitter.scalding.db.macros._
import com.twitter.scalding.db.macros.impl.handler._

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
    // pick the last apply method which (anecdotally) gives us the defaults
    // set in the case class declaration, not the companion object
    val applyList = moduleSym.typeSignature.declaration(newTermName("apply")).asTerm.alternatives
    val apply = applyList.last.asMethod
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

  private[scalding] def getColumnFormats[T](c: Context)(implicit T: c.WeakTypeTag[T]): List[ColumnFormat[c.type]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    // Field To JDBCColumn
    @tailrec
    def matchField(accessorTree: List[MethodSymbol],
      oTpe: Type,
      fieldName: FieldName,
      defaultValOpt: Option[c.Expr[String]],
      annotationInfo: List[(Type, Option[Int])],
      nullable: Boolean): scala.util.Try[List[ColumnFormat[c.type]]] = {
      oTpe match {
        // String handling
        case tpe if tpe =:= typeOf[String] => StringTypeHandler(c)(accessorTree, fieldName, defaultValOpt, annotationInfo, nullable)
        case tpe if tpe =:= typeOf[Short] => NumericTypeHandler(c)(accessorTree, fieldName, defaultValOpt, annotationInfo, nullable, "SMALLINT")
        case tpe if tpe =:= typeOf[Int] => NumericTypeHandler(c)(accessorTree, fieldName, defaultValOpt, annotationInfo, nullable, "INT")
        case tpe if tpe =:= typeOf[Long] => NumericTypeHandler(c)(accessorTree, fieldName, defaultValOpt, annotationInfo, nullable, "BIGINT")
        case tpe if tpe =:= typeOf[Double] => NumericTypeHandler(c)(accessorTree, fieldName, defaultValOpt, annotationInfo, nullable, "DOUBLE")
        case tpe if tpe =:= typeOf[Boolean] => NumericTypeHandler(c)(accessorTree, fieldName, defaultValOpt, annotationInfo, nullable, "BOOLEAN")
        case tpe if tpe =:= typeOf[java.util.Date] => DateTypeHandler(c)(accessorTree, fieldName, defaultValOpt, annotationInfo, nullable)
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && nullable == true =>
          Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName} which contains a nested option. This is not supported by this macro."))

        case tpe if tpe.erasure =:= typeOf[Option[Any]] && nullable == false =>
          if (defaultValOpt.isDefined)
            Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, with a default value. Options cannot have default values"))
          else {
            matchField(accessorTree, tpe.asInstanceOf[TypeRefApi].args.head, fieldName, None, annotationInfo, true)
          }
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(accessorTree, tpe)

        // default
        case _ => Failure(new Exception(s"Case class ${T.tpe} has field ${fieldName}: ${oTpe.toString}, which is not supported for talking to JDBC"))
      }
    }

    def expandMethod(outerAccessorTree: List[MethodSymbol], outerTpe: Type): scala.util.Try[List[ColumnFormat[c.type]]] = {
      val defaultArgs = getDefaultArgs(c)(outerTpe)

      // Intializes the type info
      outerTpe.declarations.foreach(_.typeSignature)

      // We have to build this up front as if the case class definition moves to another file
      // the annotation moves from the value onto the getter method?
      val annotationData: Map[String, List[(Type, List[Tree])]] = outerTpe
        .declarations
        .map { m =>
          val mappedAnnotations = m.annotations.map(t => (t.tpe, t.scalaArgs))
          m.name.toString.trim -> mappedAnnotations
        }.groupBy(_._1).map {
          case (k, l) =>
            (k, l.map(_._2).reduce(_ ++ _))
        }.filter {
          case (_, v) =>
            v.nonEmpty
        }

      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .map { m =>
          val fieldName = m.name.toTermName.toString.trim
          val defaultVal = defaultArgs.get(fieldName)

          val annotationInfo: List[(Type, Option[Int])] = annotationData.getOrElse(m.name.toString.trim, Nil)
            .collect {
              case (tpe, List(Literal(Constant(siz: Int)))) if tpe =:= typeOf[com.twitter.scalding.db.macros.size] => (tpe, Some(siz))
              case (tpe, _) if tpe =:= typeOf[com.twitter.scalding.db.macros.size] => c.abort(c.enclosingPosition, "Hit a size macro where we couldn't parse the value. Probably not a literal constant. Only literal constants are supported.")
              case (tpe, _) if tpe <:< typeOf[com.twitter.scalding.db.macros.ScaldingDBAnnotation] => (tpe, None)
            }
          (m, fieldName, defaultVal, annotationInfo)
        }
        .map {
          case (accessorMethod, fieldName, defaultVal, annotationInfo) =>
            matchField(outerAccessorTree :+ accessorMethod, accessorMethod.returnType, FieldName(fieldName), defaultVal, annotationInfo, false)
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

    val formats = expandMethod(Nil, T.tpe) match {
      case Success(s) => s
      case Failure(e) => (c.abort(c.enclosingPosition, e.getMessage))
    }

    val duplicateFields = formats
      .map(_.fieldName)
      .groupBy(identity)
      .filter(_._2.size > 1)
      .keys

    if (duplicateFields.nonEmpty) {
      c.abort(c.enclosingPosition,
        s"""
        Duplicate field names found: ${duplicateFields.mkString(",")}.
        Please check your nested case classes.
        """)
    } else {
      formats
    }
  }

  def getColumnDefn[T](c: Context)(implicit T: c.WeakTypeTag[T]): List[c.Expr[ColumnDefinition]] = {
    import c.universe._

    val columnFormats = getColumnFormats[T](c)

    columnFormats.map {
      case cf: ColumnFormat[_] =>
        val nullableVal = if (cf.nullable)
          q"_root_.com.twitter.scalding.db.Nullable"
        else
          q"_root_.com.twitter.scalding.db.NotNullable"
        val fieldTypeSelect = Select(q"_root_.com.twitter.scalding.db", newTermName(cf.fieldType))
        val res = q"""new _root_.com.twitter.scalding.db.ColumnDefinition(
        $fieldTypeSelect,
        _root_.com.twitter.scalding.db.ColumnName(${cf.fieldName.toStr}),
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
        // MySQL uses names like `DATE`, `INTEGER` and `VARCHAR`;
        // Vertica uses names like `Date`, `Integer` and `Varchar`
        val typeName = q"""
        val $typeNameTerm = $rsmdTerm.getColumnTypeName(${pos + 1}).toUpperCase(java.util.Locale.US)
        """
        // certain types have synonyms, so we group them together here
        // note: this is mysql specific
        // http://dev.mysql.com/doc/refman/5.0/en/numeric-type-overview.html
        val typeValidation = cf.fieldType match {
          case "VARCHAR" => q"""List("VARCHAR", "CHAR").contains($typeNameTerm)"""
          case "BOOLEAN" | "TINYINT" => q"""List("BOOLEAN", "BOOL", "TINYINT").contains($typeNameTerm)"""
          case "INT" => q"""List("INTEGER", "INT").contains($typeNameTerm)"""
          // In Vertica, `INTEGER`, `INT`, `BIGINT`, `INT8`, `SMALLINT`, and `TINYINT` are all 64 bits
          // https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/DataTypes/Numeric/INTEGER.htm
          // In MySQL, `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, and `BIGINT` are all <= 64 bits
          // https://dev.mysql.com/doc/refman/5.7/en/integer-types.html
          // As the user has told us this field can store a `BIGINT`, we can safely accept any of these
          // types from the database.
          case "BIGINT" =>
            q"""List("INTEGER", "INT", "BIGINT", "INT8", "SMALLINT",
               "TINYINT", "SMALLINT", "MEDIUMINT").contains($typeNameTerm)"""
          case f => q"""$f == $typeNameTerm"""
        }
        val typeAssert = q"""
        if (!$typeValidation) {
          throw new _root_.com.twitter.scalding.db.JdbcValidationException(
            "Mismatched type for column '" + $fieldName + "'. Expected " + ${cf.fieldType} +
              " but set to " + $typeNameTerm + " in DB.")
        }
        """
        val nullableTerm = newTermName(c.fresh(s"isNullable_$pos"))
        val nullableValidation = q"""
        val $nullableTerm = $rsmdTerm.isNullable(${pos + 1})
        if ($nullableTerm == _root_.java.sql.ResultSetMetaData.columnNoNulls && ${cf.nullable}) {
          throw new _root_.com.twitter.scalding.db.JdbcValidationException(
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
        val (box: Option[Tree], primitiveGetter: Tree) = cf.fieldType match {
          case "VARCHAR" | "TEXT" =>
            (None, q"""$rsTerm.getString($fieldName)""")
          case "BOOLEAN" | "TINYINT" =>
            (Some(q"""_root_.java.lang.Boolean.valueOf"""), q"""$rsTerm.getBoolean($fieldName)""")
          case "DATE" | "DATETIME" =>
            (None, q"""Option($rsTerm.getTimestamp($fieldName)).map { ts => new java.util.Date(ts.getTime) }.orNull""")
          // dates set to null are populated as None by tuple converter
          // if the corresponding case class field is an Option[Date]
          case "DOUBLE" =>
            (Some(q"""_root_.java.lang.Double.valueOf"""), q"""$rsTerm.getDouble($fieldName)""")
          case "BIGINT" =>
            (Some(q"""_root_.java.lang.Long.valueOf"""), q"""$rsTerm.getLong($fieldName)""")
          case "INT" | "SMALLINT" =>
            (Some(q"""_root_.java.lang.Integer.valueOf"""), q"""$rsTerm.getInt($fieldName)""")
          case f =>
            (None, q"""sys.error("Invalid format " + $f + " for " + $fieldName)""")
        }
        // note: UNSIGNED BIGINT is currently unsupported
        val valueTerm = newTermName(c.fresh("colValue"))
        val boxed = box.map { b => q"""$b($valueTerm)""" }.getOrElse(q"""$valueTerm""")
        // primitiveGetter needs to be invoked before we can use wasNull
        // to check if the column value that was read is null or not
        q"""
          { val $valueTerm = $primitiveGetter; if ($rsTerm.wasNull) null else $boxed }
        """
      }
    }
    val tcTerm = newTermName(c.fresh("conv"))
    val res = q"""
    new _root_.com.twitter.scalding.db.ResultSetExtractor[$T] {
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
    new _root_.com.twitter.scalding.db.ColumnDefinitionProvider[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override val columns = List(..$columns)
      override val resultSetExtractor = $resultSetExtractor
    }
    """
    c.Expr[ColumnDefinitionProvider[T]](res)
  }
}
