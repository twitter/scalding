package com.twitter.scalding.macros

import scala.language.experimental.macros

import com.twitter.scalding._
import com.twitter.scalding.macros.impl._

object MacroImplicits {
  /**
   * This method provides proof that the given type is a case class.
   */
  implicit def materializeCaseClassTupleSetter[T]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterImpl[T]
  implicit def materializeCaseClassTupleConverter[T]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterImpl[T]
}
