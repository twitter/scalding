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

// String form of a Column definition to be understood by a db
case class Definition(toStr: String) extends AnyVal

object DBColumnDefinition {
  def apply(col: ColumnDefinition): DBColumnDefinition = DBColumnDefinition(col.jdbcType,
    col.name,
    col.nullable,
    col.sizeOpt,
    col.defaultValue,
    SqlTypeName(col.jdbcType.toString))
}

case class DBColumnDefinition(jdbcType: SqlType,
  name: ColumnName,
  nullable: IsNullable,
  sizeOpt: Option[Int],
  defaultValue: Option[String],
  sqlType: SqlTypeName)

object DBColumnTransformer {
  def columnDefnToDefinition(col: ColumnDefinition,
    columnMutator: PartialFunction[DBColumnDefinition, DBColumnDefinition]): Definition = {
    val preparedCol = columnMutator(DBColumnDefinition(col))
    val sizeStr = preparedCol.sizeOpt.map { siz => s"($siz)" }.getOrElse("")
    val defStr = preparedCol.defaultValue.map { default => s" DEFAULT '${default}' " }.getOrElse(" ")
    val sqlType = preparedCol.sqlType.toStr

    Definition(sqlType + sizeStr + defStr + preparedCol.nullable.toStr)
  }

  private def defaultColumnMutator: PartialFunction[DBColumnDefinition, DBColumnDefinition] = {
    case t @ DBColumnDefinition(BIGINT, _, _, None, _, _) => t.copy(sizeOpt = Some(20))
    case t @ DBColumnDefinition(INT, _, _, None, _, _) => t.copy(sizeOpt = Some(11))
    case t @ DBColumnDefinition(SMALLINT, _, _, None, _, _) => t.copy(sizeOpt = Some(6))
    case t @ DBColumnDefinition(TINYINT, _, _, None, _, _) => t.copy(sizeOpt = Some(6))
    case t @ DBColumnDefinition(VARCHAR, _, _, None, _, _) => t.copy(sizeOpt = Some(255))
    case t => t
  }

  def mutateColumns(columnMutator: PartialFunction[DBColumnDefinition, DBColumnDefinition],
    columns: Iterable[ColumnDefinition]): Iterable[DBColumnDefinition] =
    columns.map(c => columnMutator.orElse(defaultColumnMutator)(DBColumnDefinition(c)))

  def columnDefnsToCreate(columnMutator: PartialFunction[DBColumnDefinition, DBColumnDefinition],
    columns: Iterable[ColumnDefinition]): Iterable[Definition] =
    columns.map(c => columnDefnToDefinition(c, columnMutator.orElse(defaultColumnMutator)))

  def columnDefnsToCreate(columns: Iterable[ColumnDefinition]): Iterable[Definition] =
    columns.map(c => columnDefnToDefinition(c, defaultColumnMutator))
}
