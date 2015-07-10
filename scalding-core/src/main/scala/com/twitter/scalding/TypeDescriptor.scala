
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

import cascading.tuple.Fields
import scala.language.experimental.{ macros => sMacros }

/**
 * This class is used to bind together a Fields instance which may contain a type array via getTypes,
 * a TupleConverter and TupleSetter, which are inverses of one another. Note the size of the Fields
 * object and the arity values for the converter and setter are all the same. Note in the scalding-macros
 * package there are macros to generate this for case classes, which may be very convenient.
 */

trait TypeDescriptor[T] extends java.io.Serializable {
  def setter: TupleSetter[T]
  def converter: TupleConverter[T]
  def fields: Fields
}
object TypeDescriptor {
  /**
   * This type descriptor flattens tuples and case classes left to right,
   * depth first. It supports any type T where T is Boolean, String,
   * Short, Int, Long, Float or Double, or an Option of these,
   * or a tuple of a supported type. So, ((Int, Int), Int) is supported,
   * and is flattened into a length 3 cascading Tuple/Fields.
   * ((Int, Int), (Int, Int)) would be a length 4 cascading tuple,
   * similarly with case classes.
   * Note, the Fields types are populated at the end of this with the
   * exception that Option[T] is recorded as Object (since recording it
   * as the java type would have different consequences for Cascading's
   * null handling.
   */
  implicit def typeDescriptor[T]: TypeDescriptor[T] = macro com.twitter.scalding.macros.impl.TypeDescriptorProviderImpl.caseClassTypeDescriptorImpl[T]
}
