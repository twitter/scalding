package com.twitter.scalding_internal.db.macros.impl

import scala.language.experimental.macros

import scala.reflect.macros.Context

import com.twitter.scalding_internal.db.macros.impl.upstream.bijection.IsCaseClassImpl
import com.twitter.scalding_internal.db.DBTypeDescriptor
import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding_internal.db.macros.impl.upstream.scalding._

object DBTypeDescriptorImpl {

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[DBTypeDescriptor[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    val columnDefn = ColumnDefinitionProviderImpl[T](c)
    val converter = TupleConverterImpl.caseClassTupleConverterWithUnknownImpl[T](c)
    val setter = TupleSetterImpl.caseClassTupleSetterWithUnknownImpl[T](c)
    val jdbcSetter = JdbcStatementSetterImpl.caseClassJdbcStatementSetterWithUnknownImpl[T](c)
    val fields = FieldsProviderImpl.toFieldsWithUnknownNoPrefixImpl[T](c)

    val res = q"""
    new _root_.com.twitter.scalding_internal.db.DBTypeDescriptor[$T] with _root_.com.twitter.scalding_internal.db.macros.upstream.bijection.MacroGenerated {
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
