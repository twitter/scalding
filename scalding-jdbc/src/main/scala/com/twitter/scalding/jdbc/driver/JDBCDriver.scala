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
import cascading.jdbc.{ MySqlScheme, JDBCScheme, TableDesc }

object JDBCDriver {

  def apply(driverName: Adapter): JDBCDriver = {
    driverName.toStr.toLowerCase match {
      case "mysql" => MysqlDriver()
      case "hsqldb" => HsqlDbDriver()
      case "vertica" => VerticaDriver()
      case "old_vertica" => OldVerticaDriver()
      case _ => sys.error("Bad driver argument given: " + driverName)
    }
  }

  def columnDefnToDefinition(col: ColumnDefinition,
    columnMutator: PartialFunction[DriverColumnDefinition, DriverColumnDefinition]): Definition = {
    val preparedCol = columnMutator(DriverColumnDefinition(col))
    val sizeStr = preparedCol.sizeOpt.map { siz => s"($siz)" }.getOrElse("")
    val defStr = preparedCol.defaultValue.map { default => s" DEFAULT '${default}' " }.getOrElse(" ")
    val sqlType = preparedCol.sqlType.toStr

    Definition(sqlType + sizeStr + defStr + preparedCol.nullable.toStr)
  }

  def defaultColumnMutator: PartialFunction[DriverColumnDefinition, DriverColumnDefinition] = {
    case t @ DriverColumnDefinition(BIGINT, _, _, None, _, _) => t.copy(sizeOpt = Some(20))
    case t @ DriverColumnDefinition(INT, _, _, None, _, _) => t.copy(sizeOpt = Some(11))
    case t @ DriverColumnDefinition(SMALLINT, _, _, None, _, _) => t.copy(sizeOpt = Some(6))
    case t @ DriverColumnDefinition(TINYINT, _, _, None, _, _) => t.copy(sizeOpt = Some(6))
    case t @ DriverColumnDefinition(VARCHAR, _, _, None, _, _) => t.copy(sizeOpt = Some(255))
    case t => t
  }

  def columnDefnsToCreate(columnMutator: PartialFunction[DriverColumnDefinition, DriverColumnDefinition],
    columns: Iterable[ColumnDefinition]): Iterable[Definition] =
    columns.map(c => columnDefnToDefinition(c, columnMutator))

}

trait JDBCDriver {
  // Must supply the driver class to be used
  def driver: DriverClass

  // Optionally supply  means to mutate the column definitions for this driver
  protected def columnMutator: PartialFunction[DriverColumnDefinition, DriverColumnDefinition] = PartialFunction.empty

  protected def colsToDefs(columns: Iterable[ColumnDefinition]) =
    JDBCDriver.columnDefnsToCreate(columnMutator.orElse(JDBCDriver.defaultColumnMutator), columns)

  // Generate SQL statement, mostly used in debugging to see how this would be created
  // or to let users manually create it themselves
  def toSqlCreateString(tableName: TableName, cols: Iterable[ColumnDefinition]): String = {
    val allCols = cols.map(_.name).zip(colsToDefs(cols))
      .map { case (ColumnName(name), Definition(defn)) => s"  `${name}`  $defn" }
      .mkString(",\n|")

    s"""
    |CREATE TABLE `${tableName.toStr}` (
    |$allCols
    |)
    |""".stripMargin('|')
  }

  def getTableDesc(
    tableName: TableName,
    columns: Iterable[ColumnDefinition]): TableDesc =
    new TableDesc(tableName.toStr, columns.map(_.name.toStr).toArray, colsToDefs(columns).map(_.toStr).toArray, null, null)

  def getJDBCScheme(
    columnNames: Iterable[ColumnName],
    filterCondition: Option[String],
    updateBy: Iterable[String],
    replaceOnInsert: Boolean): JDBCScheme = {
    if (replaceOnInsert) sys.error("replaceOnInsert functionality only supported by MySql")
    new JDBCScheme(
      null, // inputFormatClass
      null, // outputFormatClass
      columnNames.map(_.toStr).toArray,
      null, // orderBy
      filterCondition.orNull,
      updateBy.toArray)
  }
}

case class HsqlDbDriver() extends JDBCDriver {
  override val driver = DriverClass("org.hsqldb.jdbcDriver")
}

