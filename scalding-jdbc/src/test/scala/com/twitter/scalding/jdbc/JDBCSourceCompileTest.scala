package com.twitter.scalding.jdbc

import org.scalatest.WordSpec

class ExampleMysqlJdbcSource() extends JDBCSource with MysqlDriver {
  override val tableName = TableName("test")
  override val columns: Iterable[ColumnDefinition] = Iterable(
    int("hey"),
    bigint("you"),
    varchar("get"),
    datetime("off"),
    text("of"),
    double("my"),
    smallint("cloud")
  )
  override def currentConfig = ConnectionSpec(ConnectUrl("how"), UserName("are"), Password("you"))
}

class ExampleVerticaJdbcSource() extends JDBCSource with VerticaJdbcDriver {
  override val tableName = TableName("test")
  override val columns: Iterable[ColumnDefinition] = Iterable(
    int("hey"),
    bigint("you"),
    varchar("get"),
    datetime("off"),
    text("of"),
    double("my"),
    smallint("cloud")
  )
  override def currentConfig = ConnectionSpec(ConnectUrl("how"), UserName("are"), Password("you"))
}

class JDBCSourceCompileTest extends WordSpec {
  "JDBCSource" should {
    "Pick up correct column definitions for MySQL Driver" in {
      new ExampleMysqlJdbcSource().toSqlCreateString
    }
  }
}
