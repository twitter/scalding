package com.twitter.scalding_internal.db.macros

import org.mockito.Mockito.when
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.mock.MockitoSugar

import com.twitter.scalding._
import com.twitter.scalding_internal.db.macros._
import org.scalatest.exceptions.TestFailedException
import com.twitter.scalding_internal.db._

import com.twitter.scalding_internal.db.macros.upstream.bijection.{ IsCaseClass, MacroGenerated }
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

case class BadUser1(user_name: String, age: Int = 13)
case class BadUser2(@size(-1) user_name: String, age: Int)
case class BadUser3(@size(0) age: Int)
case class BadUser5(user_name: Option[String] = Some("bob"), age: Int)

case class BadUser6(user_names: List[String])
object Consts {
  val cInt: Int = 13
}
case class BadUser7(@size(Consts.cInt) age: Int)
case class BadUser8(age: Option[Option[Int]])
case class BadUser9(@size(15)@text age: Option[Option[Int]])
case class BadUser10(@size(2)@size(4) age: Option[Option[Int]])

case class ExhaustiveJdbcCaseClass(
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

class JdbcMacroUnitTests extends WordSpec with Matchers with MockitoSugar {

  val dummy = new ColumnDefinitionProvider[Nothing] {
    override val columns = Nil
    override val resultSetExtractor = new ResultSetExtractor {
      override def apply(rs: java.sql.ResultSet) = "dummy"
    }
  }

  def isColumnDefinitionAvailable[T](implicit proof: ColumnDefinitionProvider[T] = dummy.asInstanceOf[ColumnDefinitionProvider[T]]) {
    proof shouldBe a[MacroGenerated]
    proof.columns.isEmpty shouldBe false
  }

  def isJDBCTypeInfoAvailable[T](implicit proof: DBTypeDescriptor[T] = dummy.asInstanceOf[DBTypeDescriptor[T]]) {
    proof shouldBe a[MacroGenerated]
    proof.columnDefn.columns.isEmpty shouldBe false
  }

  "String field missing annotation" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser1]
  }

  "String field size annotation not in range" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser2]
  }

  "Int field size annotation not in range" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser3]
  }

  "Option field with default" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser5]
  }

  "Unknown field type" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser6]
  }

  "Annotation for size doesn't use a constant" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser7]
  }

  "Nested options should be blocked" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser8]
  }

  "Extra annotation not supported on current field " in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser9]
  }

  "Two annotations of the same type " in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[BadUser10]
  }

  "Produces the ColumnDefinition" should {

    isColumnDefinitionAvailable[User]

    val expectedColumns = List(
      ColumnDefinition(INT, ColumnName("date_id"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("user_name"), NotNullable, Some(64), None),
      ColumnDefinition(INT, ColumnName("age"), Nullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("gender"), NotNullable, Some(22), Some("male")))

    val rs = mock[java.sql.ResultSet]
    when(rs.getLong("date_id")) thenReturn (123L)
    when(rs.getString("user_name")) thenReturn ("alice")
    when(rs.getLong("age")) thenReturn (26L)
    when(rs.getString("gender")) thenReturn ("F")

    val columnDef = DBMacro.toColumnDefinitionProvider[User]
    assert(columnDef.columns.toList === expectedColumns)
    assert(columnDef.resultSetExtractor(rs) == "123\talice\t26\tF\n")
  }

  "Produces the ColumnDefinition for nested case class " should {

    isColumnDefinitionAvailable[User2]

    val expectedColumns = List(
      ColumnDefinition(INT, ColumnName("date_id"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("user_name"), NotNullable, Some(64), None),
      ColumnDefinition(INT, ColumnName("demographics.age"), Nullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("demographics.gender"), NotNullable, Some(22), Some("male")))

    assert(DBMacro.toColumnDefinitionProvider[User2].columns.toList === expectedColumns)

  }

  "Produces the DBTypeDescriptor" should {
    // explictly just call this to get a compiler error
    DBMacro.toDBTypeDescriptor[User]
    // ensure the implicit fires
    isJDBCTypeInfoAvailable[User]

    val expectedColumns = List(
      ColumnDefinition(INT, ColumnName("date_id"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("user_name"), NotNullable, Some(64), None),
      ColumnDefinition(INT, ColumnName("age"), Nullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("gender"), NotNullable, Some(22), Some("male")))

    assert(DBMacro.toDBTypeDescriptor[User].columnDefn.columns.toList === expectedColumns)

  }

  "Big Jdbc Test" should {

    isColumnDefinitionAvailable[ExhaustiveJdbcCaseClass]

    // explictly just call this to get a compiler error
    DBMacro.toDBTypeDescriptor[ExhaustiveJdbcCaseClass]
    // ensure the implicit fires
    isJDBCTypeInfoAvailable[ExhaustiveJdbcCaseClass]

    val expectedColumns = List(
      ColumnDefinition(BIGINT, ColumnName("bigInt"), NotNullable, None, None),
      ColumnDefinition(BIGINT, ColumnName("smallerInt"), NotNullable, None, None),
      ColumnDefinition(INT, ColumnName("smallerAgainInt"), NotNullable, None, None),
      ColumnDefinition(INT, ColumnName("normalIntWithSize"), NotNullable, Some(5), None),
      ColumnDefinition(SMALLINT, ColumnName("evenSmallerInt"), NotNullable, None, None),
      ColumnDefinition(DOUBLE, ColumnName("numberFun"), NotNullable, None, None),
      ColumnDefinition(BOOLEAN, ColumnName("booleanFlag"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("smallString"), NotNullable, Some(20), None),
      ColumnDefinition(VARCHAR, ColumnName("smallishString"), NotNullable, Some(200), None),
      ColumnDefinition(TEXT, ColumnName("largeString"), NotNullable, None, None),
      ColumnDefinition(TEXT, ColumnName("forceTextString"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("forcedVarChar"), NotNullable, Some(2051), None),
      ColumnDefinition(DATETIME, ColumnName("myDateWithTime"), NotNullable, None, None),
      ColumnDefinition(DATE, ColumnName("myDateWithoutTime"), NotNullable, None, None),
      ColumnDefinition(BIGINT, ColumnName("optiLong"), Nullable, None, None))

    assert(DBMacro.toColumnDefinitionProvider[ExhaustiveJdbcCaseClass].columns.toList === expectedColumns)
  }

}
