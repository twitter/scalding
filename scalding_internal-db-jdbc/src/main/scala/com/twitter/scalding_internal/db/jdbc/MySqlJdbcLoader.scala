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
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.jdbc.driver.DriverClass

import java.sql._
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

class MySqlJdbcLoader[T](
  tableName: TableName,
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition],
  batchSize: Int,
  replaceOnInsert: Boolean,
  preloadQuery: Option[SqlQuery],
  postloadQuery: Option[SqlQuery])(json2CaseClass: String => T, jdbcSetter: JdbcStatementSetter[T])
  extends JdbcLoader(tableName, None, connectionConfig, columns, preloadQuery, postloadQuery) {

  import CloseableHelper._
  import TryHelper._

  private val log = LoggerFactory.getLogger(this.getClass)

  val driverClassName = DriverClass("com.mysql.jdbc.Driver")

  def load(hadoopUri: HadoopUri): Try[Int] = {
    val insertStmt = s"""
    |INSERT INTO ${tableName.toStr} (${columns.map(_.name.toStr).mkString(",")})
    |VALUES (${Stream.continually("?").take(columns.size).mkString(",")})
    """.stripMargin('|')

    val query = if (replaceOnInsert)
      s"""
        |$insertStmt
        |ON DUPLICATE KEY UPDATE ${columns.map(_.name.toStr).map(c => s"$c=VALUES($c)").mkString(",")}
        """.stripMargin('|')
    else
      insertStmt

    log.info(s"Preparing to write from $hadoopUri to jdbc: $query")
    for {
      conn <- jdbcConnection
      ps <- Try(conn.prepareStatement(query)).onFailure(conn.closeQuietly())
      fs = FileSystem.get(new Configuration())
      files <- dataFiles(hadoopUri, fs).onFailure(ps.closeQuietly())
      loadCmds: Iterable[Try[Unit]] = files.map(processDataFile[T](_, fs, ps))
      count <- Try {
        // load files one at a time
        loadCmds.map(_.get) // throw any file load fails
        Try(ps.getUpdateCount).getOrElse(-1) // don't fail if just getting counts fails, default instead
      }.ensure {
        ps.closeQuietly()
        conn.closeQuietly()
      }
    } yield count
  }

  private def dataFiles(uri: HadoopUri, fs: FileSystem): Try[Iterable[Path]] = Try {
    val files = fs.listStatus(new Path(uri.toStr))
      .map(_.getPath)

    if (!files.exists(_.getName == "_SUCCESS"))
      sys.error(s"No SUCCESS file found in intermediate jdbc dir: ${uri.toStr}")
    else
      files.filter(_.getName != "_SUCCESS")
  }

  private def loadData[T](count: Int, it: Iterator[String], ps: PreparedStatement): Try[Unit] = {
    if (count == batchSize) {
      Try(() /*ps.executeBatch()*/ ).map(_ => loadData[T](0, it, ps))
    } else if (it.hasNext) {
      for {
        rec <- Try(json2CaseClass(it.next))
        _ <- jdbcSetter(rec, ps)
        _ <- Try(ps.addBatch())
      } yield loadData[T](count + 1, it, ps)
    } else Try()
    // Try(ps.executeBatch()) // end of data
  }

  private def processDataFile[T](p: Path, fs: FileSystem, ps: PreparedStatement): Try[Unit] =
    for {
      reader <- Try(Source.fromInputStream(fs.open(p)))
      _ <- loadData[T](0, reader.getLines(), ps).ensure(reader.close())
    } yield ()
}
