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

package com.twitter.scalding_internal.db.jdbc

import com.twitter.scalding_internal.db._

import java.sql.{ Connection, DriverManager }
import scala.util.Try

case class HadoopUri(toStr: String) extends AnyVal

abstract class JdbcLoader(
  tableName: TableName,
  schema: Option[SchemaName],
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition]) extends java.io.Serializable {

  protected def colsToDefs(columns: Iterable[ColumnDefinition]) =
    DBColumnTransformer.columnDefnsToCreate(columns)

  protected def jdbcConnection: Try[Connection] =
    Try(DriverManager.getConnection(connectionConfig.connectUrl.toStr,
      connectionConfig.userName.toStr,
      connectionConfig.password.toStr)).map { c =>
      c.setAutoCommit(true)
      c
    }

  protected val sqlTableCreateStmt = {
    val allCols = columns.map(_.name).zip(colsToDefs(columns))
      .map { case (ColumnName(name), Definition(defn)) => s"""  "${name}"  $defn""" }
      .mkString(",\n|")

    s"""
      |create TABLE IF NOT EXISTS ${schema.map(_.toStr + ".").getOrElse("")}${tableName.toStr} (
      |$allCols
      |)
      """.stripMargin('|')
  }

  protected val driverClass: Class[_] = try {
    Class.forName(driverClassName);
  } catch {
    case e: ClassNotFoundException =>
      System.err.println(s"Could not find the JDBC driver: $driverClassName");
      e.printStackTrace();
      throw e
  }

  // TODO: user JDBCDriver class instead
  def driverClassName: String

  def load(uri: HadoopUri): Try[Int]
}
