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

import java.io.IOException
import java.sql.ResultSet

import cascading.flow.FlowProcess
import cascading.jdbc.JDBCTap
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.tuple.{ Fields, TupleEntry, TupleEntryCollector }
import cascading.util.Util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

import com.twitter.scalding._
import com.twitter.scalding_internal.db._

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
 *     MysqlDriver
 *   )
 * }
 *
 * @author Ian O Connell
 */

abstract class TypedJDBCSource[T: DBTypeDescriptor](dbsInEnv: AvailableDatabases) extends JDBCSource(dbsInEnv) with TypedSource[T] with TypedSink[T] with Mappable[T] {
  private val jdbcTypeInfo = implicitly[DBTypeDescriptor[T]]
  val columns = jdbcTypeInfo.columnDefn.columns
  private val resultSetExtractor = jdbcTypeInfo.columnDefn.resultSetExtractor
  override def fields: Fields = jdbcTypeInfo.fields
  override def sinkFields = jdbcTypeInfo.fields
  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](jdbcTypeInfo.converter)
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](jdbcTypeInfo.setter)

  private def hdfsScheme = HadoopSchemeInstance(new TextDelimited(sourceFields,
    null, false, false, "\t", true, "\"", sourceFields.getTypesClasses, true)
    .asInstanceOf[Scheme[_, _, _, _, _]])

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    (mode, readOrWrite) match {
      case (Hdfs(_, conf), Read) => {
        val hfsTap = new JdbcSourceHfsTap(hdfsScheme, initTemporaryPath(new JobConf(conf)))
        val rs2String: (ResultSet => String) = resultSetExtractor.toTsv(_)
        JdbcToHdfsCopier(connectionConfig, toSqlSelectString, hfsTap.getPath,
          TextDelimited.DEFAULT_CHARSET)(rs2String)
        CastHfsTap(hfsTap)
      }
      case _ => super.createTap(readOrWrite)
    }

  protected def initTemporaryPath(conf: JobConf): String =
    new Path(Hfs.getTempPath(conf),
      "jdbc-hdfs-" + Util.createUniqueID().substring(0, 5)).toString
}

private[this] class JdbcSourceHfsTap(scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _], stringPath: String)
  extends Hfs(scheme, stringPath) {
  override def openForWrite(flowProcess: FlowProcess[JobConf],
    input: OutputCollector[_, _]): TupleEntryCollector =
    throw new IOException("Writing not supported")
}

