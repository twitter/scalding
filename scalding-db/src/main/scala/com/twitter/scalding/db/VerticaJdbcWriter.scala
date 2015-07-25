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

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

import com.twitter.scalding.db.extensions.VerticaExtensions
import com.twitter.scalding.db.driver.DriverClass

import java.sql._
import java.util.Properties
import scala.util.{ Try, Success, Failure }

class VerticaJdbcWriter(tableName: TableName,
  schema: SchemaName,
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition],
  addlQueries: AdditionalQueries)
  extends JdbcWriter(tableName, connectionConfig, columns, addlQueries) {

  import CloseableHelper._
  import TryHelper._

  private val log = LoggerFactory.getLogger(this.getClass)

  protected[this] val driverClassName = DriverClass("com.vertica.jdbc.Driver")

  override def colsToDefs(columns: Iterable[ColumnDefinition]) =
    DBColumnTransformer.columnDefnsToCreate(VerticaExtensions.verticaMutator, columns)

  private def runCmd(conn: Connection, sql: String): Try[Int] = {
    val statement = conn.createStatement
    println("Executing sql: \n" + sql + "\n")
    Try(statement.execute(sql))
      .map { _ => Try(statement.getUpdateCount).getOrElse(-1) }
      .onComplete(statement.close())
  }

  protected def sqlTableCreateStmt: SqlQuery = {
    val allCols = columns.map(_.name).zip(colsToDefs(columns))
      .map { case (ColumnName(name), Definition(defn)) => s"""  ${name}  $defn""" }
      .mkString(",\n|")

    SqlQuery(s"""
      |create TABLE IF NOT EXISTS ${schema.toStr}.${tableName.toStr} (
      |$allCols
      |)
      """.stripMargin('|'))
  }

  override protected def createTableIfNotExists: Try[Unit] =
    runQuery(SqlQuery(sqlTableCreateStmt.toStr))

  def load(hadoopUri: HadoopUri, conf: JobConf): Try[Int] = {

    val fs = FileSystem.get(conf)
    val federatedName = fs.resolvePath(new Path(hadoopUri.toStr)).toString.replaceAll("hdfs://", "").split("/")(0)

    val runningAsUserName = System.getProperty("user.name")
    for {
      webhdfsUrl <- HdfsUtil.webhdfsUrl(federatedName, conf)
      httpHdfsUrl = s"""${webhdfsUrl}${hadoopUri.toStr}/part-*"""
      conn <- jdbcConnection
      _ <- runCmd(conn, sqlTableCreateStmt.toStr).onFailure(conn.close())
      loadSqlStatement = s"""COPY ${schema.toStr}.${tableName.toStr} NATIVE with SOURCE public.Hdfs(url='$httpHdfsUrl', username='$runningAsUserName') ABORT ON ERROR"""
      // abort on error - if any single row has a schema mismatch, vertica rolls back the transaction and fails
      loadedCount <- runCmd(conn, loadSqlStatement).onComplete(conn.close())
    } yield loadedCount
  }
}
