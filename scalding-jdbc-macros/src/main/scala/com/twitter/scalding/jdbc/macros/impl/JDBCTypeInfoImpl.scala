package com.twitter.scalding.jdbc.macros.impl

import scala.language.experimental.macros

import scala.reflect.macros.Context

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.jdbc.JDBCTypeInfo
import com.twitter.scalding.jdbc.macros._
import com.twitter.scalding.macros.impl._

object JDBCTypeInfoImpl {

  def caseClassJDBCTypeInfoImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[JDBCTypeInfo[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    val columnDefn = JDBCMacroImpl.caseClassJDBCPayloadImpl[T](c)
    val converter = TupleConverterImpl.caseClassTupleConverterWithUnknownImpl[T](c)
    val setter = TupleSetterImpl.caseClassTupleSetterWithUnknownImpl[T](c)
    val fields = FieldsProviderImpl.toFieldsWithUnknownImpl[T](c)

    val res = q"""
    new _root_.com.twitter.scalding.jdbc.JDBCTypeInfo[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override val columnDefn = $columnDefn
      override val converter = $converter
      override val setter = $setter
      override val fields = $fields
    }
    """
    c.Expr[JDBCTypeInfo[T]](res)
  }
}
