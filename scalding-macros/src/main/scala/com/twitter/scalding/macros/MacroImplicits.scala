package com.twitter.scalding.macros

import scala.language.experimental.macros

import com.twitter.scalding._
import com.twitter.scalding.macros.impl.MacroImpl

object MacroImplicits {
  /**
   * This method provides proof that the given type is a case class.
   */
  implicit def isCaseClass[T]: IsCaseClass[T] = macro MacroImpl.isCaseClassImpl[T]
  implicit def materializeCaseClassTupleSetter[T: IsCaseClass]: TupleSetter[T] = macro MacroImpl.caseClassTupleSetterImpl[T]
  implicit def materializeCaseClassTupleConverter[T: IsCaseClass]: TupleConverter[T] = macro MacroImpl.caseClassTupleConverterImpl[T]
}
