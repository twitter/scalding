package com.twitter.scalding_internal.db.jdbc

import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.macros._

import org.scalatest.WordSpec

case class User(
  date_id: Int,
  @size(64) user_name: String,
  age: Option[Int],
  @size(22) gender: String = "male")

case class ExampleTypedJDBCSource extends TypedJDBCSource[User](AvailableDatabases(Map(Database("asdf") -> ConnectionConfig(ConnectUrl("how"), UserName("are"), Password("you"), Adapter("mysql"))))) {
  override val database = Database("asdf")
  override val tableName = TableName("test")

  override def transformInTest = false
}

class TypedJDBCSourceCompileTest extends WordSpec {
  "TypedJDBCSource" should {
    "Pick up correct column definitions for MySQL Driver" in {
      val expectedCreate = """
        |CREATE TABLE `test` (
        |  `date_id`  INT(11) NOT NULL,
        |  `user_name`  VARCHAR(64) NOT NULL,
        |  `age`  INT(11) NULL,
        |  `gender`  VARCHAR(22) DEFAULT 'male' NOT NULL
        |)
        |""".stripMargin('|')
      assert(ExampleTypedJDBCSource().toSqlCreateString === expectedCreate)
    }
  }
}
