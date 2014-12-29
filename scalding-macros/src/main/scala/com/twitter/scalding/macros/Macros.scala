package com.twitter.scalding.macros

import scala.language.experimental.macros

import com.twitter.scalding._
import com.twitter.scalding.macros.impl._
import com.twitter.bijection.macros.IsCaseClass
import cascading.tuple.Fields

object Macros {
  // These only work for simple types inside the case class
  // Nested case classes are allowed, but only: Int, Boolean, String, Long, Short, Float, Double of other types are allowed
  def caseClassTupleSetter[T: IsCaseClass]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterImpl[T]
  def caseClassTupleConverter[T: IsCaseClass]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterImpl[T]
  def toFields[T: IsCaseClass]: Fields = macro FieldsProviderImpl.toFieldsImpl[T]
  def toNamedFields[T: IsCaseClass]: Fields = macro FieldsProviderImpl.toFieldsImpl[T]
  def toIndexedFields[T: IsCaseClass]: Fields = macro FieldsProviderImpl.toIndexedFieldsImpl[T]
}
