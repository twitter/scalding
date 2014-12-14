package com.twitter.scalding.macros

import scala.language.experimental.macros

import com.twitter.scalding._
import com.twitter.scalding.macros.impl.MacroImpl
import com.twitter.bijection.macros.IsCaseClass

object Macros {
  def caseClassTupleSetter[T: IsCaseClass]: TupleSetter[T] = macro MacroImpl.caseClassTupleSetterImpl[T]
  def caseClassTupleConverter[T: IsCaseClass]: TupleConverter[T] = macro MacroImpl.caseClassTupleConverterImpl[T]
}
