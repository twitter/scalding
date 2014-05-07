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

package com.twitter.scalding

import java.lang.reflect.Type

import cascading.tuple.Fields

trait FieldsTyper[Z] {
  def apply(fields: Fields): Fields = {
    val comparables = (0 until fields.size).map { fields.get(_) }.toArray
    val types = getListOfClasses
    require(comparables.size == types.size, "arity of types must match that of field.\n" +
      "Comparables were: " + comparables + "\nTypes were: " + types)
    new Fields(comparables, types)
  }
  protected def getListOfClasses: Array[Type]
}

object FieldsTyper extends GeneratedFieldsTypers {
}

trait LowPriorityFieldsTypers extends LowerPriorityFieldsTypers {
  implicit def singleTyper[T](implicit t: Manifest[T]): FieldsTyper[T] = new FieldsTyper[T] {
    override def getListOfClasses = Array[Type](t.erasure)
  }
}

trait LowerPriorityFieldsTypers extends java.io.Serializable {
  // TODO would prefer this not to be implicit... bigger fish to fry in the short term.
  // Eventually can turn on the implicit resolution flag and see where it is getting used.
  implicit def identityTyper[T]: FieldsTyper[T] = new FieldsTyper[T] {
    override def apply(fields: Fields) = fields
    override def getListOfClasses = throw new UnsupportedOperationException("identityTyper has no classes")
  }
}
