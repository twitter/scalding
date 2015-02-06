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

import java.sql._
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

class MySqlJdbcLoader[T](
  tableName: TableName,
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition],
  batchSize: Int,
  replaceOnInsert: Boolean)(json2CaseClass: String => T, jdbcSetter: JdbcStatementSetter[T])
  extends JdbcLoader(tableName, None, connectionConfig, columns) {

  private val log = LoggerFactory.getLogger(this.getClass)

  val driverClassName = "com.mysql.jdbc.Driver"

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
      ps <- Try(conn.prepareStatement(query))
      fs = FileSystem.get(new Configuration())
      files <- dataFiles(hadoopUri, fs)
    } yield {
      files.foreach(processDataFile[T](_, fs, ps))
      val count = Try(ps.getUpdateCount).getOrElse(-1)
      ps.close()
      conn.close()
      count
    }
  }

  private def dataFiles(uri: HadoopUri, fs: FileSystem) = Try {
    val files = fs.listStatus(new Path(uri.toStr))
      .map(_.getPath)

    if (!files.exists(_.getName == "_SUCCESS"))
      sys.error(s"No SUCCESS file found in intermediate jdbc dir: ${uri.toStr}")
    else
      files.filter(_.getName != "_SUCCESS")
  }

  @tailrec
  private def execute[T](count: Int, it: Iterator[String], ps: PreparedStatement): Unit = {
    if (count == batchSize) {
      ps.executeBatch()
      execute[T](0, it, ps)
    } else if (it.hasNext) {
      val rec = json2CaseClass(it.next)
      jdbcSetter(rec, ps).get // throw on failure
      ps.addBatch()
      execute[T](count + 1, it, ps)
    } else
      ps.executeBatch()
  }

  private def processDataFile[T](p: Path, fs: FileSystem, ps: PreparedStatement) = {
    val reader = Source.fromInputStream(fs.open(p))
    execute[T](0, reader.getLines(), ps)
    reader.close()
  }
}
