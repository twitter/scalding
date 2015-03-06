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
import org.apache.hadoop.mapred.JobConf
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
abstract class JdbcWriter(
  tableName: TableName,
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
      c.setAutoCommit(false) // explicitly call commit/rollback for transactions
      c
    }

  /** Query to create table */
  protected def sqlTableCreateStmt: SqlQuery

  /** Create table if it does not exist. Used before write operation. */
  protected def createTableIfNotExists: Try[Unit]

  protected def driverClass: Class[_] = try {
    Class.forName(driverClassName.toStr);
  } catch {
    case e: ClassNotFoundException =>
      log.error(s"Could not find the JDBC driver: $driverClassName", e);
      throw e
  }

  protected[this] def driverClassName: DriverClass

  protected def load(uri: HadoopUri, conf: JobConf): Try[Int]

  protected def getStatement(conn: Connection): Try[Statement] =
    Try(conn.createStatement())

  protected def successFlagCheck(uri: HadoopUri, conf: JobConf): Try[Unit] = Try {
    val fs = FileSystem.newInstance(conf)
    val files = fs.listStatus(new Path(uri.toStr))
      .map(_.getPath)
    if (!files.exists(_.getName == "_SUCCESS"))
      sys.error(s"No SUCCESS file found in intermediate jdbc dir: ${uri.toStr}")
    fs.close()
  }

  // perform the overall write to jdbc operation
  final def run(uri: HadoopUri, conf: JobConf): Try[Int] =
    for {
      _ <- Try(driverClass)
      _ <- createTableIfNotExists
      _ <- addlQueries.preload.map(runQuery).getOrElse(Try())
      _ <- successFlagCheck(uri, conf)
      count <- load(uri, conf)
      _ <- addlQueries.postload.map(runQuery).getOrElse(Try())
    } yield count

  def runQuery(query: SqlQuery): Try[Unit] =
    for {
      conn <- jdbcConnection
      stmt <- getStatement(conn).onFailure(conn.closeQuietly())
      _ <- Try { stmt.execute(query.toStr); conn.commit() }
        .onFailure { conn.rollback() }
        .onComplete { stmt.closeQuietly(); conn.closeQuietly() }
    } yield ()
}
