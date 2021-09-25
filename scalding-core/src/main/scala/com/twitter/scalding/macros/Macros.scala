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

  // There is two flavors of the below functions, the pure vs withUnknown.
  // In both cases recursive case classes, primitive types, and options are flattened down onto cascading tuples.
  // In the unknown casehowever if a type is reached that we don't know what to do we store that type into the tuple.

  def caseClassTupleSetter[T]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterImpl[T]
  def caseClassTupleSetterWithUnknown[T]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterWithUnknownImpl[T]

  def caseClassTupleConverter[T]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterImpl[T]
  def caseClassTupleConverterWithUnknown[T]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterWithUnknownImpl[T]

  def toFields[T]: Fields = macro FieldsProviderImpl.toFieldsImpl[T]
  def toFieldsWithUnknown[T]: Fields = macro FieldsProviderImpl.toFieldsWithUnknownImpl[T]

  def toNamedFields[T]: Fields = macro FieldsProviderImpl.toFieldsImpl[T]
  def toNamedFieldsWithUnknown[T]: Fields = macro FieldsProviderImpl.toFieldsWithUnknownImpl[T]

  def toIndexedFields[T]: Fields = macro FieldsProviderImpl.toIndexedFieldsImpl[T]
  def toIndexedFieldsWithUnknown[T]: Fields = macro FieldsProviderImpl.toIndexedFieldsWithUnknownImpl[T]

  def caseClassTypeDescriptor[T]: TypeDescriptor[T] = macro TypeDescriptorProviderImpl.caseClassTypeDescriptorImpl[T]
  def caseClassTypeDescriptorWithUnknown[T]: TypeDescriptor[T] = macro TypeDescriptorProviderImpl.caseClassTypeDescriptorWithUnknownImpl[T]
}
