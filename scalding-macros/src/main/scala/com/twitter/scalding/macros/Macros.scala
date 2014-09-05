package com.twitter.scalding.macros

import scala.language.experimental.macros

import com.twitter.scalding._
import com.twitter.scalding.macros.impl.MacroImpl

object Macros {
  def caseClassTupleSetter[T: IsCaseClass]: TupleSetter[T] = macro MacroImpl.caseClassTupleSetterImpl[T]
  def caseClassTupleConverter[T: IsCaseClass]: TupleConverter[T] = macro MacroImpl.caseClassTupleConverterImpl[T]
}

/**
 * This trait is meant to be used exclusively to allow the type system to prove that a class is or is not a case class.
 */
trait IsCaseClass[T]
