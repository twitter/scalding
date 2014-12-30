package com.twitter.scalding.macros

import scala.language.experimental.macros

import com.twitter.scalding._
import com.twitter.scalding.macros.impl._
import cascading.tuple.Fields

object Macros {
  // These only work for simple types inside the case class
  // Nested case classes are allowed, but only: Int, Boolean, String, Long, Short, Float, Double of other types are allowed
  def caseClassTupleSetter[T]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterImpl[T]
  def caseClassTupleConverter[T]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterImpl[T]
  def toFields[T]: Fields = macro FieldsProviderImpl.toFieldsImpl[T]
  def toNamedFields[T]: Fields = macro FieldsProviderImpl.toFieldsImpl[T]
  def toIndexedFields[T]: Fields = macro FieldsProviderImpl.toIndexedFieldsImpl[T]
}
