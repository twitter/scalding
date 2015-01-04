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

package com.twitter.scalding.jdbc

case class ColumnName(toStr: String) extends AnyVal
case class SqlTypeName(toStr: String) extends AnyVal

case class ColumnDefinition(jdbcType: JdbcType,
  name: ColumnName,
  nullable: IsNullable,
  sizeOpt: Option[Int],
  defaultValue: Option[String]) extends Serializable

trait ColumnDefinitionProvider[T] extends Serializable {
  def columns: Iterable[ColumnDefinition]
}

sealed trait JdbcType
case object BIGINT extends JdbcType
case object INT extends JdbcType
case object SMALLINT extends JdbcType
case object TINYINT extends JdbcType
case object VARCHAR extends JdbcType
case object DATE extends JdbcType
case object DATETIME extends JdbcType
case object TEXT extends JdbcType
case object DOUBLE extends JdbcType

object IsNullable {
  def apply(isNullable: Boolean): IsNullable = if (isNullable) Nullable else NotNullable
}

sealed abstract class IsNullable(val toStr: String)
case object Nullable extends IsNullable("NULL")
case object NotNullable extends IsNullable("NOT NULL")

trait ColumnDefiner {
  // Some helper methods that we can use to generate column definitions
  protected def bigint(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(BIGINT, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def int(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(INT, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def smallint(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(SMALLINT, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def tinyint(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(TINYINT, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def varchar(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(VARCHAR, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def date(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(DATE, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def datetime(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(DATETIME, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def text(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(TEXT, ColumnName(name), nullable, sizeOpt, defaultValue)

  protected def double(name: String, nullable: IsNullable = NotNullable, sizeOpt: Option[Int] = None, defaultValue: Option[String] = None) =
    ColumnDefinition(DOUBLE, ColumnName(name), nullable, sizeOpt, defaultValue)
}