/*
 Copyright 2014 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
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
