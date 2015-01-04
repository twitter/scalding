package com.twitter.scalding.jdbc

import org.scalatest.WordSpec

class ExampleJdbcSource(adapter: Adapter) extends JDBCSource {
  override val tableName = TableName("test")
  override val columns: Iterable[ColumnDefinition] = Iterable(
    int("hey"),
    bigint("you"),
    varchar("get"),
    datetime("off"),
    text("of"),
    double("my"),
    smallint("cloud"))
  override def connectionConfig = ConnectionConfig(ConnectUrl("how"), UserName("are"), Password("you"), adapter)
}

class JDBCSourceCompileTest extends WordSpec {
  "JDBCSource" should {
    "Pick up correct column definitions for MySQL Driver" in {
      val expectedCreate = """
        |CREATE TABLE `test` (
        |  `hey`  INT(11) NOT NULL,
        |  `you`  BIGINT(20) NOT NULL,
        |  `get`  VARCHAR(255) NOT NULL,
        |  `off`  DATETIME NOT NULL,
        |  `of`  TEXT NOT NULL,
        |  `my`  DOUBLE NOT NULL,
        |  `cloud`  SMALLINT(6) NOT NULL
        |)
        |""".stripMargin('|')
      assert(new ExampleJdbcSource(Adapter("mysql")).toSqlCreateString === expectedCreate)
    }

    "Pick up correct column definitions for Vertica Driver" in {
      val expectedCreate = """
        |CREATE TABLE `test` (
        |  `hey`  INT NOT NULL,
        |  `you`  BIGINT NOT NULL,
        |  `get`  VARCHAR(255) NOT NULL,
        |  `off`  DATETIME NOT NULL,
        |  `of`  TEXT NOT NULL,
        |  `my`  DOUBLE PRECISION NOT NULL,
        |  `cloud`  SMALLINT NOT NULL
        |)
        |""".stripMargin('|')
      assert(new ExampleJdbcSource(Adapter("vertica")).toSqlCreateString === expectedCreate)
    }
  }
}
