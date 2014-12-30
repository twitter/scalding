/*
Copyright 2012 Twitter, Inc.

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

package com.twitter.scalding.jdbc.driver

import com.twitter.scalding.jdbc._

case class Definition(toStr: String) extends AnyVal
case class DriverClass(toStr: String) extends AnyVal
case class SqlTypeName(toStr: String) extends AnyVal

object DriverColumnDefinition {
  def apply(col: ColumnDefinition): DriverColumnDefinition = DriverColumnDefinition(col.jdbcType,
    col.name,
    col.nullable,
    col.sizeOpt,
    col.defaultValue,
    SqlTypeName(col.jdbcType.toString))
}

case class DriverColumnDefinition(jdbcType: JdbcType,
  name: ColumnName,
  nullable: IsNullable,
  sizeOpt: Option[Int],
  defaultValue: Option[String],
  sqlType: SqlTypeName)

