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

package com.twitter.scalding_internal.db.jdbc.driver
import com.twitter.scalding_internal.db._
import cascading.jdbc.{ MySqlScheme, JDBCScheme, TableDesc }

case class MysqlDriver() extends JDBCDriver {
  override val driver = DriverClass("com.mysql.jdbc.Driver")

  override def getTableDesc(
    tableName: TableName,
    columns: Iterable[ColumnDefinition]): TableDesc =
    new TableDesc(
      tableName.toStr,
      columns.map(_.name.toStr).toArray,
      colsToDefs(columns).map(_.toStr).toArray,
      null,
      "SHOW TABLES LIKE '%s'")

  override def getJDBCScheme(
    columnNames: Iterable[ColumnName],
    filterCondition: Option[String],
    updateBy: Iterable[String],
    replaceOnInsert: Boolean) = {
    new MySqlScheme(
      null, // inputFormatClass
      columnNames.map(_.toStr).toArray,
      null, // orderBy
      filterCondition.orNull,
      updateBy.toArray,
      replaceOnInsert)
  }
}
