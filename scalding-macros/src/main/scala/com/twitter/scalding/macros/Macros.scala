package com.twitter.scalding.macros

import scala.language.experimental.macros

import com.twitter.scalding._
import com.twitter.scalding.macros.impl.MacroImpl
import com.twitter.bijection.macros.IsCaseClass

object Macros {
  // These only work for simple types inside the case class
  // Nested case classes are allowed, but only: Int, Boolean, String, Long, Short, Float, Double of other types are allowed
  def caseClassTupleSetter[T: IsCaseClass]: TupleSetter[T] = macro MacroImpl.caseClassTupleSetterImpl[T]
  def caseClassTupleConverter[T: IsCaseClass]: TupleConverter[T] = macro MacroImpl.caseClassTupleConverterImpl[T]
}
