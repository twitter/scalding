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

import com.twitter.scalding.{ AccessMode, Hdfs, Mode, Source, TestTapFactory }
import cascading.jdbc.JDBCTap
import cascading.tap.Tap
import cascading.tuple.Fields
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
object JDBCTapBuilder {
  def build(cols: Iterable[ColumnDefinition], jdbcOptions: JDBCOptions) =
    try {
      val ConnectionConfig(url, uName, passwd, jdbcDriverName, _) = jdbcOptions.connectionConfig
      val jdbcDriver = JDBCDriver(jdbcDriverName)

      val tableDesc = jdbcDriver.getTableDesc(jdbcOptions.tableName, cols)

      val jdbcScheme = jdbcDriver.getJDBCScheme(cols.map(_.name),
        jdbcOptions.filterCondition,
        jdbcOptions.updateBy,
        jdbcOptions.replaceOnInsert)

      val tap = new JDBCTap(
        url.toStr,
        uName.toStr,
        passwd.toStr,
        jdbcDriver.driver.toStr,
        tableDesc,
        jdbcScheme)

      tap.setConcurrentReads(jdbcOptions.maxConcurrentReads)
      tap.setBatchSize(jdbcOptions.batchSize)
      tap
    } catch {
      case e: NullPointerException => sys.error("Could not find DB credential information.")
    }
}
