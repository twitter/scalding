package com.twitter.scalding.jdbc

import cascading.jdbc.{ MySqlScheme, JDBCScheme, TableDesc }

case class DriverClass(get: String)

trait JdbcDriver {
  def driver: DriverClass
  def getTableDesc(
    tableName: TableName,
    columnNames: Array[ColumnName],
    columnDefinitions: Array[Definition]) =
    new TableDesc(tableName.get, columnNames.map(_.get), columnDefinitions.map(_.get), null, null)
  def getJDBCScheme(
    columnNames: Array[ColumnName],
    filterCondition: Option[String],
    updateBy: Iterable[String],
    replaceOnInsert: Boolean) = {
    if (replaceOnInsert) sys.error("replaceOnInsert functionality only supported by MySql")
    new JDBCScheme(
      null, // inputFormatClass
      null, // outputFormatClass
      columnNames.map(_.get),
      null, // orderBy
      filterCondition.orNull,
      updateBy.toArray)
  }
}

trait MysqlDriver extends JdbcDriver with MysqlTableCreationImplicits {
  override val driver = DriverClass("com.mysql.jdbc.Driver")
  override def getTableDesc(
    tableName: TableName,
    columnNames: Array[ColumnName],
    columnDefinitions: Array[Definition]) =
    new TableDesc(
      tableName.get,
      columnNames.map(_.get),
      columnDefinitions.map(_.get),
      null,
      "SHOW TABLES LIKE '%s'")
  override def getJDBCScheme(
    columnNames: Array[ColumnName],
    filterCondition: Option[String],
    updateBy: Iterable[String],
    replaceOnInsert: Boolean) = {
    new MySqlScheme(
      null, // inputFormatClass
      columnNames.map(_.get),
      null, // orderBy
      filterCondition.orNull,
      updateBy.toArray,
      replaceOnInsert)
  }
}

trait HsqlDbDriver extends JdbcDriver {
  override val driver = DriverClass("org.hsqldb.jdbcDriver")
}

/**
 * Old Vertica 4.1 jdbc driver
 */
trait VerticaDriver extends JdbcDriver with VerticaTableCreationImplicits {
  override val driver = DriverClass("com.vertica.Driver")
}

/**
 * Vertica jdbc driver (5.1 and higher)
 */
trait VerticaJdbcDriver extends JdbcDriver with VerticaTableCreationImplicits {
  override val driver = DriverClass("com.vertica.jdbc.Driver")
}
