package com.twitter.scalding.db.macros.impl

import scala.reflect.macros.Context

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import com.twitter.scalding.macros.impl.{ FieldsProviderImpl, TupleConverterImpl, TupleSetterImpl }
import com.twitter.scalding.db.DBTypeDescriptor

object DBTypeDescriptorImpl {

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[DBTypeDescriptor[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    val columnDefn = ColumnDefinitionProviderImpl[T](c)
    val converter = TupleConverterImpl.caseClassTupleConverterWithUnknownImpl[T](c)
    val setter = TupleSetterImpl.caseClassTupleSetterWithUnknownImpl[T](c)
    val jdbcSetter = JdbcStatementSetterImpl.caseClassJdbcSetterCommonImpl[T](c, true)
    val fields = FieldsProviderImpl.toFieldsWithUnknownNoPrefixImpl[T](c)

    val res = q"""
    new _root_.com.twitter.scalding.db.DBTypeDescriptor[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override val columnDefn = $columnDefn
      override val converter = $converter
      override val setter = $setter
      override val fields = $fields
      override val jdbcSetter = $jdbcSetter
    }
    """
    c.Expr[DBTypeDescriptor[T]](res)
  }
}
