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

package com.twitter.scalding.db

import com.twitter.scalding.TupleConverter

case class ColumnName(toStr: String) extends AnyVal
case class SqlTypeName(toStr: String) extends AnyVal

case class ColumnDefinition(jdbcType: SqlType,
  name: ColumnName,
  nullable: IsNullable,
  sizeOpt: Option[Int],
  defaultValue: Option[String]) extends Serializable

trait ColumnDefinitionProvider[T] extends Serializable {
  def columns: Iterable[ColumnDefinition]
  def resultSetExtractor: ResultSetExtractor[T]
}

class JdbcValidationException(msg: String) extends RuntimeException(msg)

trait ResultSetExtractor[T] {
  def validate(rsmd: java.sql.ResultSetMetaData): scala.util.Try[Unit]
  def toCaseClass(rs: java.sql.ResultSet, c: TupleConverter[T]): T
}
