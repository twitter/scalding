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
import java.sql.{ ResultSet, ResultSetMetaData }
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

import cascading.flow.FlowProcess
import cascading.jdbc.JDBCTap
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.hadoop.{ TextLine => CHTextLine }
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.tuple.{ Fields, Tuple, TupleEntry, TupleEntryCollector, TupleEntryIterator }
import cascading.util.Util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

import com.twitter.bijection.Injection
import com.twitter.scalding._
import com.twitter.scalding_internal.db._

import JsonUtils._

/**
 * Extend this source to let scalding read from or write to a database.
 * In order for this to work you need to specify the table name, column definitions and DB credentials.
 * If you write to a DB, the fields in the final pipe have to correspond to the column names in the DB table.
 *
 * NOTE: Currently, only MySQL is supported.
 *
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

abstract class TypedJDBCSource[T <: AnyRef: DBTypeDescriptor: Manifest](dbsInEnv: AvailableDatabases)
  extends JDBCSource(dbsInEnv)
  with TypedSource[T]
  with TypedSink[T]
  with Mappable[T]
  with JDBCLoadOptions {
  import Dsl._

  private val jdbcTypeInfo = implicitly[DBTypeDescriptor[T]]
  val columns = jdbcTypeInfo.columnDefn.columns
  override def fields: Fields = jdbcTypeInfo.fields
  private def jdbcSetter[U <: T] = jdbcTypeInfo.jdbcSetter

  private val resultSetExtractor = jdbcTypeInfo.columnDefn.resultSetExtractor

  // override this if you want to limit the number of records per part file
  def maxRecordsPerFile: Option[Int] = None

  // override this to disable db schema validation during reads
  def dbSchemaValidation: Boolean = true

  // override this to be QueryOnMappers if you want to query the db directly from mappers
  // it can be used in conjunction with maxConcurrentReads to control the number of mappers
  // use QueryOnMappers only if you have a very large dataset that needs to be read in splits to be efficient
  // for most cases, QueryOnSubmitter works better and is safer
  def queryPolicy: QueryPolicy = QueryOnSubmitter

  private def hdfsScheme = HadoopSchemeInstance(new CHTextLine(CHTextLine.DEFAULT_SOURCE_FIELDS, CHTextLine.DEFAULT_CHARSET)
    .asInstanceOf[Scheme[_, _, _, _, _]])

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    (mode, readOrWrite, queryPolicy) match {
      case (Hdfs(_, conf), Read, QueryOnMappers) => super.createTap(Read) // uses cascading's JDBCTap
      case (Hdfs(_, conf), Read, QueryOnSubmitter) => {
        // copy to hdfs via submitter. uses json for the snapshot
        val hfsTap = new JdbcSourceHfsTap(hdfsScheme, initTemporaryPath(new JobConf(conf)))
        val rs2CaseClass: (ResultSet => T) = resultSetExtractor.toCaseClass(_, jdbcTypeInfo.converter)
        val validator: Option[ResultSetMetaData => Try[Unit]] =
          if (dbSchemaValidation)
            Some(resultSetExtractor.validate(_))
          else
            None
        JdbcToHdfsCopier(connectionConfig, toSqlSelectString, hfsTap.getPath,
          CHTextLine.DEFAULT_CHARSET, maxRecordsPerFile)(validator, rs2CaseClass)
        CastHfsTap(hfsTap)
      }
      case (Hdfs(_, conf), Write, _) => {
        val writePath = initTemporaryPath(new JobConf(conf))
        CastHfsTap(new JdbcSinkHfsTap(hdfsScheme, writePath, completionHandler))
      }
      case _ => TestTapFactory(this, hdfsScheme).createTap(readOrWrite)
    }

  @transient private[this] lazy val inj: Injection[T, String] = caseClass2Json[T]

  @transient lazy val mysqlWriter = new MySqlJdbcWriter[T](
    tableName,
    connectionConfig,
    columns,
    batchSize,
    replaceOnInsert,
    AdditionalQueries(preloadQuery, postloadQuery))(inj.invert(_).get, jdbcTypeInfo.jdbcSetter)

  @transient lazy val completionHandler = new JdbcSinkCompletionHandler(mysqlWriter)

  // we use transform methods because setter and converter methods
  // do not have access to Mode for special casing unit test code paths
  override def transformForRead(pipe: Pipe) =
    queryPolicy match {
      case QueryOnMappers => pipe // no transform needed when using cascading's JDBCTap
      case QueryOnSubmitter =>
        // convert snapshot records from json to T
        // we pick field 1 because 0 is offset in TextLine
        pipe.mapTo(1 -> 0) { t: String =>
          inj.invert(t).get
        }
    }

  // use json for output data to hdfs prior to loading to mysql
  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo(0 -> 0) { k: T =>
      inj.apply(k)
    }

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U] {
    queryPolicy match {
      case QueryOnMappers => jdbcTypeInfo.converter // use default converter
      case QueryOnSubmitter => TupleConverter.singleConverter[T]
    }
  }

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

  protected def initTemporaryPath(conf: JobConf): String =
    new Path(Hfs.getTempPath(conf),
      "jdbc-hdfs-" + Util.createUniqueID().substring(0, 5)).toString
  // using substring because of hadoop limits on filename length
}

private[this] class JdbcSourceHfsTap(scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _], stringPath: String)
  extends Hfs(scheme, stringPath) {
  override def openForWrite(flowProcess: FlowProcess[JobConf],
    input: OutputCollector[_, _]): TupleEntryCollector =
    throw new IOException("Writing not supported")
}
