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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.slf4j.LoggerFactory

import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.jdbc.driver.DriverClass

import java.sql.{ Connection, DriverManager, Statement }
import scala.util.Try

case class AdditionalQueries(
  preload: Option[SqlQuery],
  postload: Option[SqlQuery])

/**
 * Enables inserting data into JDBC compatible databases (MySQL, Vertica, etc)
 * by streaming via submitter after the MR job has run and output data has been staged in HDFS.
 */
abstract class JdbcLoader(
  tableName: TableName,
  schema: Option[SchemaName],
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition],
  addlQueries: AdditionalQueries) extends java.io.Serializable {

  import CloseableHelper._
  import TryHelper._

  private val log = LoggerFactory.getLogger(this.getClass)

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

  protected def driverClass: Class[_] = try {
    Class.forName(driverClassName.toStr);
  } catch {
    case e: ClassNotFoundException =>
      System.err.println(s"Could not find the JDBC driver: $driverClassName");
      e.printStackTrace();
      throw e
  }

  protected[this] def driverClassName: DriverClass

  protected def load(uri: HadoopUri): Try[Int]

  protected def getStatement(conn: Connection): Try[Statement] =
    Try(conn.createStatement())

  protected def successFlagCheck(uri: HadoopUri): Try[Unit] = Try {
    val fs = FileSystem.get(new Configuration())
    val files = fs.listStatus(new Path(uri.toStr))
      .map(_.getPath)
    if (!files.exists(_.getName == "_SUCCESS"))
      sys.error(s"No SUCCESS file found in intermediate jdbc dir: ${uri.toStr}")
  }

  final def runLoad(uri: HadoopUri): Try[Int] =
    for {
      _ <- Try(driverClass)
      _ <- addlQueries.preload.map(runQuery).getOrElse(Try())
      _ <- successFlagCheck(uri)
      count <- load(uri)
      _ <- addlQueries.postload.map(runQuery).getOrElse(Try())
    } yield count

  def runQuery(query: SqlQuery): Try[Unit] =
    for {
      conn <- jdbcConnection
      stmt <- getStatement(conn).onFailure(conn.closeQuietly())
      _ <- Try(stmt.execute(query.toStr))
        .onComplete { stmt.closeQuietly(); conn.closeQuietly() }
    } yield ()
}
