/*
Copyright 2015 Twitter, Inc.

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

import org.scalatest.WordSpec

class ExampleJdbcSource(adapter: Adapter) extends JDBCSource(AvailableDatabases(Map(Database("asdf") -> ConnectionConfig(ConnectUrl("how"), UserName("are"), Password("you"), adapter, StringEncoding("UTF8"))))) {
  override val database = Database("asdf")
  override val tableName = TableName("test")
  override val columns: Iterable[ColumnDefinition] = Iterable(
    int("hey"),
    bigint("you"),
    varchar("get"),
    datetime("off"),
    text("of"),
    double("my"),
    smallint("cloud"))
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
