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

package com.twitter.scalding.jdbc

import com.twitter.scalding.{ AccessMode, Hdfs, Mode, Source, TestTapFactory }
import cascading.jdbc.JDBCTap
import cascading.tap.Tap
import cascading.tuple.Fields

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
 *   override def currentConfig = ConnectionSpec(
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
abstract class JDBCSource extends Source with ColumnDefiner with JdbcDriver {

  // Override the following three members when you extend this class
  val tableName: TableName
  val columns: Iterable[ColumnDefinition]
  protected def currentConfig: ConnectionSpec

  // Must be a subset of column names.
  // If updateBy column names are given, a SQL UPDATE statement will be generated
  // if the values in those columns for the given Tuple are all not {@code null}.
  // Otherwise an INSERT statement will be generated.
  val updateBy: Iterable[String] = Nil

  // The body of a WHERE clause. If present will filter the full table by this condition.
  val filterCondition: Option[String] = None

  // Override this if your table is really large
  def maxConcurrentReads = 1

  // How many rows to insert/update into this table in a batch?
  def batchSize = 1000

  // If true, will perform an update when inserting a row with a primary or unique key that already
  // exists in the table. Will replace the old values in that row with the new values.
  val replaceOnInsert: Boolean = false

  def fields: Fields = new Fields(columnNames.map(_.get).toSeq: _*)

  protected def createJDBCTap =
    try {
      val ConnectionSpec(url, uName, passwd) = currentConfig
      val tap = new JDBCTap(
        url.get,
        uName.get,
        passwd.get,
        driver.get,
        getTableDesc(tableName, columnNames, columnDefinitions),
        getJDBCScheme(columnNames, filterCondition, updateBy, replaceOnInsert))
      tap.setConcurrentReads(maxConcurrentReads)
      tap.setBatchSize(batchSize)
      tap
    } catch {
      case e: NullPointerException => sys.error("Could not find DB credential information.")
    }

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    mode match {
      case Hdfs(_, _) => createJDBCTap.asInstanceOf[Tap[_, _, _]]
      // TODO: support Local mode here, and better testing.
      case _ => TestTapFactory(this, fields).createTap(readOrWrite)
    }

  // Generate SQL statement to create the DB table if not existing.
  def toSqlCreateString: String = {
    def addBackTicks(str: String) = "`" + str + "`"
    val allCols = columns
      .map { case ColumnDefinition(ColumnName(name), Definition(defn)) => addBackTicks(name) + " " + defn }
      .mkString(",\n")

    "CREATE TABLE " + addBackTicks(tableName.get) + " (\n" + allCols + ",\n PRIMARY KEY HERE!!!!"
  }
}

case class TableName(get: String)

