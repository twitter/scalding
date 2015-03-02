package com.twitter.scalding_internal.db.vertica.macros

import org.mockito.Mockito.{ reset, when }
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.exceptions.TestFailedException
import org.scalatest.mock.MockitoSugar

import cascading.tuple.{ Fields, Tuple, TupleEntry }

import com.twitter.scalding._
import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.macros.upstream.bijection.{ IsCaseClass, MacroGenerated }

import java.sql.{ ResultSet, ResultSetMetaData }
import java.util.Date

case class User(
  date_id: Int,
  @size(64) user_name: String,
  age: Option[Int],
  @size(22) gender: String = "male")

case class Demographics(
  age: Option[Int],
  @size(22) gender: String = "male")

case class User2(
  date_id: Int,
  @size(64) user_name: String,
  demographics: Demographics)

case class ExhaustiveJdbcCaseClass(
  bigInt: Long, // 8 bytes
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

case class CaseClassWithDate(
  id: Long,
  myDateWithTime: Date,
  @date myDateWithoutTime: Date)

case class CaseClassWithOptions(
  id: Option[Int],
  @size(20) name: Option[String],
  date_id: Option[Date])

class JdbcMacroUnitTests extends WordSpec with Matchers with MockitoSugar {

  "Produces the VerticaRowSerializer" should {

    VerticaRowSerializerProvider[Demographics]
    VerticaRowSerializerProvider[User]
    VerticaRowSerializerProvider[User2]
    VerticaRowSerializerProvider[CaseClassWithDate]
    VerticaRowSerializerProvider[CaseClassWithOptions]
    VerticaRowSerializerProvider[ExhaustiveJdbcCaseClass]
    assert(true != false)
  }
}
