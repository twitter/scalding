/*
 Copyright 2015 Twitter, Inc.

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

import scala.reflect.macros.Context
import scala.util.Try

/**
 * Helper to set fields from a case class to other "container" types
 * E.g. cascading Tuple, jdbc PreparedStatement
 */
trait CaseClassFieldSetter {

  // mark the field as absent/null
  def absent(c: Context)(idx: Int, container: c.TermName): c.Tree

  // use the default field setter (for when there is no type-specific setter)
  def default(c: Context)(idx: Int, container: c.TermName, fieldValue: c.Tree): c.Tree

  // use the field setter known specific to the given field type
  // return scala.util.Failure if no type specific setter in the container
  def from(c: Context)(fieldType: c.Type, idx: Int, container: c.TermName, fieldValue: c.Tree): Try[c.Tree]
}
