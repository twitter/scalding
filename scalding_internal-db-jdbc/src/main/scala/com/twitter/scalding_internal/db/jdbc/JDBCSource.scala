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

package com.twitter.scalding_internal.db.jdbc

import cascading.flow.FlowProcess
import cascading.jdbc.JDBCTap
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.tuple.{ Fields, TupleEntryCollector, TupleEntryIterator }
import cascading.util.Util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import org.slf4j.LoggerFactory

import com.twitter.scalding._
import com.twitter.scalding_internal.db.jdbc.driver.JDBCDriver
import com.twitter.scalding_internal.db._

import java.io.IOException
import java.sql._
import scala.util.{ Failure, Success, Try }

/**
 * Extend this source to let scalding read from or write to a database.
 * In order for this to work you need to specify the table name, column definitions and DB credentials.
 * If you write to a DB, the fields in the final pipe have to correspond to the column names in the DB table.
 * Example usage:
 * case object YourTableSource extends JDBCSource {
 *   override val tableName = TableName("tableName")
 *   override val columns = List(
 *      varchar("col1", 64),
 *      date("col2"),
 *      tinyint("col3"),
 *      double("col4")
 *   )
 *   override def currentConfig = ConnectionConfig(
 *     ConnectUrl("jdbc:mysql://mysql01.company.com:3306/production"),
 *     UserName("username"), Password("password"),
 *     MysqlDriver
 *   )
 * }
 *
 * @author Argyris Zymnis
 * @author Oscar Boykin
 * @author Kevin Lin
 */

abstract class JDBCSource(dbsInEnv: AvailableDatabases) extends Source with JDBCOptions with ColumnDefiner {

  override val availableDatabases: AvailableDatabases = dbsInEnv

  val columns: Iterable[ColumnDefinition]
  val resultSetExtractor: ResultSetExtractor

  protected val log = LoggerFactory.getLogger(this.getClass)

  def fields: Fields = new Fields(columns.map(_.name.toStr).toSeq: _*)

  protected def hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    (mode, readOrWrite) match {
      case (Hdfs(_, conf), Read) => {
        val hfsTap = new JdbcSourceHfsTap(hdfsScheme, initTemporaryPath(new JobConf(conf)))
        log.info("Starting copy to hdfs staging")
        val rs2String = (rs: ResultSet) => resultSetExtractor(rs)
        JdbcToHdfsCopier(connectionConfig, getSelectQuery, hfsTap.getPath)(rs2String)
        CastHfsTap(hfsTap)
      }
      case (Hdfs(_, _), Write) => JDBCTapBuilder.build(columns, this).asInstanceOf[Tap[_, _, _]]
      // TODO: support Local mode here, and better testing.
      case _ => TestTapFactory(this, fields).createTap(readOrWrite)
    }

  // SQL statement for debugging what this source would produce to create the table
  // Can also be used for a user to create the table themselves. Setting up indices in the process.
  def toSqlCreateString: String = JDBCDriver(connectionConfig.adapter).toSqlCreateString(tableName, columns)

  protected def getSelectQuery: String = {
    val query = new StringBuilder
    query
      .append("SELECT ")
      .append(columns.map(_.name.toStr).mkString(", "))
      .append(" FROM ")
      .append(tableName.toStr)
    filterCondition.foreach { c =>
      query.append(" WHERE ").append(c)
    }
    query.toString
  }

  protected def initTemporaryPath(conf: JobConf): String =
    new Path(Hfs.getTempPath(conf),
      "jdbc-" + tableName.toStr + "-" + Util.createUniqueID().substring(0, 5)).toString
}

class JdbcSourceHfsTap(scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _], stringPath: String)
  extends Hfs(scheme, stringPath) {
  override def openForWrite(flowProcess: FlowProcess[JobConf],
    input: OutputCollector[_, _]): TupleEntryCollector =
    throw new IOException("Writing not supported")
}

object JdbcToHdfsCopier {

  protected val log = LoggerFactory.getLogger(this.getClass)

  def apply(connectionConfig: ConnectionConfig, selectQuery: String, hdfsPath: Path)(rs2String: ResultSet => String): Unit = {
    Try(DriverManager.getConnection(connectionConfig.connectUrl.toStr,
      connectionConfig.userName.toStr,
      connectionConfig.password.toStr)).map { conn =>
      val fsconf = new Configuration
      val fs = FileSystem.get(fsconf)
      val hdfsStagingFile = fs.create(new Path(hdfsPath + "/part-00000"))
      val stmt = conn.createStatement
      stmt.setFetchSize(Integer.MIN_VALUE) // don't pull entire table into memory
      log.info(s"Executing query $selectQuery")
      val rs = stmt.executeQuery(selectQuery)
      while (rs.next) {
        val output = rs2String(rs)
        hdfsStagingFile.write(s"$output\n".getBytes)
      }
      hdfsStagingFile.close
    } match {
      case Success(s) => s
      case Failure(e) => throw new java.lang.IllegalArgumentException(s"Failed - ${e.getMessage}", e)
    }
  }
}
