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

package com.twitter.scalding.db

import cascading.flow.FlowProcess
import cascading.jdbc.JDBCTap
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.{ Fields, TupleEntryCollector, TupleEntryIterator }

import com.twitter.scalding._
import com.twitter.scalding.db.driver.JDBCDriver

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

  def fields: Fields = new Fields(columns.map(_.name.toStr).toSeq: _*)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    mode match {
      case Hdfs(_, _) => JDBCTapBuilder.build(columns, this).asInstanceOf[Tap[_, _, _]]
      // TODO: support Local mode here, and better testing.
      case _ => TestTapFactory(this, fields).createTap(readOrWrite)
    }

  // SQL statement for debugging what this source would produce to create the table
  // Can also be used for a user to create the table themselves. Setting up indices in the process.
  def toSqlCreateString: String = JDBCDriver(connectionConfig.adapter).toSqlCreateString(tableName, columns)

  protected def toSqlSelectString: String = {
    val columnsStr = columns.map(_.name.toStr).mkString(", ")
    val filterStr = filterCondition.map(c => s"WHERE $c").getOrElse("")
    s"""SELECT $columnsStr FROM ${tableName.toStr} $filterStr"""
  }
}
