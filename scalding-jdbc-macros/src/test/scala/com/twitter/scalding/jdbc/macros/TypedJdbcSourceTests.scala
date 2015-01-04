package com.twitter.scalding.jdbc.macros

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.scalding.jdbc.macros._
import org.scalatest.exceptions.TestFailedException
import com.twitter.scalding.jdbc._

import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import java.util.Date

case class TypedSource1(
  date_id: Int,
  @size(64) user_name: String,
  age: Option[Int],
  @size(22) gender: String = "male")
case class TypedSource1JdbcSource() extends TypedJDBCSource[TypedSource1](TableName("helloWorld"), ConnectionConfig(ConnectUrl("how"), UserName("are"), Password("you"), Adapter("mysql")))

case class TypedSource2(
  bigInt: scala.math.BigInt, // >8bytes
  smallerInt: Long, // 8 bytes
  smallerAgainInt: Int, // 4 bytes
  @size(5) normalIntWithSize: Int, // Sizes on numerics seem to just be for display. Not sure if its worth allowing.
  evenSmallerInt: Short, // 2 bytes
  numberFun: Double,
  booleanFlag: Boolean, // 1 byte -- tinyint
  @size(20) smallString: String, // Should goto varchar
  @size(200) smallishString: String, // Should goto varchar
  @size(2048) largeString: String, // Should goto TEXT
  @text forceTextString: String, // Force smaller to text, stored out of the table. So row query speed possibly faster
  @size(2051)@varchar forcedVarChar: String, // Forced inline to table -- only some sql version support > 255 for varchar
  myDateWithTime: Date, // Default goes to MySQL DateTime/Timestamp so its not lossy
  @date myDateWithoutTime: Date,
  optiLong: Option[Long] // Nullable long
  )

case class TypedSource2JdbcSource() extends TypedJDBCSource[TypedSource2](TableName("helloWorld2"), ConnectionConfig(ConnectUrl("how"), UserName("are"), Password("you"), Adapter("mysql")))
