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
package com.twitter.scalding.macros.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.scalding._
import com.twitter.bijection.macros.IsCaseClass
import com.twitter.bijection.macros.impl.IsCaseClassImpl
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object TypeDescriptorProviderImpl {

  def caseClassTypeDescriptorImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TypeDescriptor[T]] =
    caseClassTypeDescriptorCommonImpl(c, false)(T)

  def caseClassTypeDescriptorWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TypeDescriptor[T]] =
    caseClassTypeDescriptorCommonImpl(c, true)(T)

  def caseClassTypeDescriptorCommonImpl[T](c: Context, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[TypeDescriptor[T]] = {
    import c.universe._

    val converter = TupleConverterImpl.caseClassTupleConverterCommonImpl[T](c, allowUnknownTypes)
    val setter = TupleSetterImpl.caseClassTupleSetterCommonImpl[T](c, allowUnknownTypes)
    val fields = FieldsProviderImpl.toFieldsCommonImpl[T](c, NamedWithPrefix, allowUnknownTypes)

    val res = q"""
    new _root_.com.twitter.scalding.TypeDescriptor[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override val converter = $converter
      override val setter = $setter
      override val fields = $fields
    }
    """
    c.Expr[TypeDescriptor[T]](res)
  }

}
