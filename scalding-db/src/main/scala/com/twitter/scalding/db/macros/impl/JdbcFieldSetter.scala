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
package com.twitter.scalding.db.macros.impl

import scala.reflect.macros.Context
import scala.util.Try

import com.twitter.scalding.macros.impl.CaseClassFieldSetter

/**
 * Helper class for setting case class fields in java.sql.Statement
 */
private[macros] object JdbcFieldSetter extends CaseClassFieldSetter {

  override def absent(c: Context)(idx: Int, container: c.TermName): c.Tree = {
    import c.universe._
    q"""$container.setObject($idx + 1, null)"""
  }

  override def default(c: Context)(idx: Int, container: c.TermName, fieldValue: c.Tree): c.Tree = {
    import c.universe._
    q"""$container.setObject($idx + 1, $fieldValue)"""
  }

  override def from(c: Context)(fieldType: c.Type, idx: Int, container: c.TermName, fieldValue: c.Tree): Try[c.Tree] = Try {
    import c.universe._

    // jdbc Statement indexes are one-based, hence +1 here
    def simpleType(accessor: Tree) = q"""${accessor}(${idx + 1}, $fieldValue)"""

    fieldType match {
      case tpe if tpe =:= typeOf[String] => simpleType(q"$container.setString")
      case tpe if tpe =:= typeOf[Boolean] => simpleType(q"$container.setBoolean")
      case tpe if tpe =:= typeOf[Short] => simpleType(q"$container.setShort")
      case tpe if tpe =:= typeOf[Int] => simpleType(q"$container.setInt")
      case tpe if tpe =:= typeOf[Long] => simpleType(q"$container.setLong")
      case tpe if tpe =:= typeOf[Float] => simpleType(q"$container.setFloat")
      case tpe if tpe =:= typeOf[Double] => simpleType(q"$container.setDouble")
      case _ => sys.error(s"Unsupported primitive type ${fieldType}")
    }
  }
}
