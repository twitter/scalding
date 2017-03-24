package com.twitter.scalding.db.macros

import org.mockito.Mockito.{ reset, when }
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.exceptions.TestFailedException
import org.scalatest.mock.MockitoSugar

import cascading.tuple.{ Fields, Tuple, TupleEntry }

import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.scalding._
import com.twitter.scalding.db.macros._
import com.twitter.scalding.db._

import java.sql.{ ResultSet, ResultSetMetaData }
import java.util.Date

object User {
  // these defaults should not get picked up in ColumnDefinition
  def apply(): User = User(0, "username", Some(0), "female")
  def apply(date_id: Int): User = User(date_id, "username", Some(0), "female")
  def apply(date_id: Int, username: String): User = User(date_id, username, Some(0), "female")
  def apply(date_id: Int, username: String, age: Option[Int]): User = User(date_id, username, age, "female")
}

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

private final case class VerticaCaseClass(
  verticaLong: Long,
  @date verticaDate: Date,
  @varchar @size(size = 1) verticaVarchar1: String)

case class CaseClassWithDate(
  id: Long,
  myDateWithTime: Date,
  @date myDateWithoutTime: Date)

case class CaseClassWithOptions(
  id: Option[Int],
  @size(20) name: Option[String],
  date_id: Option[Date],
  boolean_value: Option[Boolean],
  short_value: Option[Short],
  long_value: Option[Long],
  double_value: Option[Double])

case class InnerWithBadNesting(
  age: Int,
  id: Long)

case class OuterWithBadNesting(
  id: Int, // duplicate in nested case class
  @text name: String,
  details: InnerWithBadNesting)

class JdbcMacroUnitTests extends WordSpec with Matchers with MockitoSugar {

  val dummy = new ColumnDefinitionProvider[Nothing] {
    override val columns = Nil
    override val resultSetExtractor = null
  }

  def isColumnDefinitionAvailable[T](implicit proof: ColumnDefinitionProvider[T] = dummy.asInstanceOf[ColumnDefinitionProvider[T]]): Unit = {
    proof shouldBe a[MacroGenerated]
    proof.columns.isEmpty shouldBe false
  }

  def isJDBCTypeInfoAvailable[T](implicit proof: DBTypeDescriptor[T] = dummy.asInstanceOf[DBTypeDescriptor[T]]): Unit = {
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

    // verify defaults are from case class declaration, not companion object
    val expectedColumns = List(
      ColumnDefinition(INT, ColumnName("date_id"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("user_name"), NotNullable, Some(64), None),
      ColumnDefinition(INT, ColumnName("age"), Nullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("gender"), NotNullable, Some(22), Some("male")))

    val typeDesc = DBMacro.toDBTypeDescriptor[User]
    val columnDef = typeDesc.columnDefn
    assert(columnDef.columns.toList === expectedColumns)

    val expectedFields = new Fields("date_id", "user_name", "age", "gender")
    assert(typeDesc.fields.equalsFields(expectedFields))

    val rsmd = mock[ResultSetMetaData]
    when(rsmd.getColumnTypeName(1)) thenReturn ("INT")
    when(rsmd.isNullable(1)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(2)) thenReturn ("VARCHAR")
    when(rsmd.isNullable(2)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(3)) thenReturn ("INT")
    when(rsmd.isNullable(3)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(4)) thenReturn ("VARCHAR")
    when(rsmd.isNullable(4)) thenReturn (ResultSetMetaData.columnNullableUnknown)

    assert(columnDef.resultSetExtractor.validate(rsmd).isSuccess)

    val rs = mock[ResultSet]
    when(rs.getInt("date_id")) thenReturn (123)
    when(rs.getString("user_name")) thenReturn ("alice")
    when(rs.getInt("age")) thenReturn (26)
    when(rs.getString("gender")) thenReturn ("F")

    assert(columnDef.resultSetExtractor.toCaseClass(rs, typeDesc.converter) == User(123, "alice", Some(26), "F"))
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
  }

  "Produces the ColumnDefinition for nested case class " should {

    isColumnDefinitionAvailable[User2]

    val expectedColumns = List(
      ColumnDefinition(INT, ColumnName("date_id"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("user_name"), NotNullable, Some(64), None),
      ColumnDefinition(INT, ColumnName("age"), Nullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("gender"), NotNullable, Some(22), Some("male")))

    val typeDesc = DBMacro.toDBTypeDescriptor[User2]
    val columnDef = typeDesc.columnDefn
    assert(columnDef.columns.toList === expectedColumns)

    val expectedFields = new Fields("date_id", "user_name", "age", "gender")
    assert(typeDesc.fields.equalsFields(expectedFields))

    val rs = mock[ResultSet]
    when(rs.getInt("date_id")) thenReturn (123)
    when(rs.getString("user_name")) thenReturn ("alice")
    when(rs.getInt("age")) thenReturn (26)
    when(rs.getString("gender")) thenReturn ("F")

    assert(columnDef.resultSetExtractor.toCaseClass(rs, typeDesc.converter) == User2(123, "alice", Demographics(Some(26), "F")))
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
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
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
  }

  "interoperates with Vertica, which uses different type names" should {
    val typeDescriptor = DBMacro.toDBTypeDescriptor[VerticaCaseClass]
    val expectedColumns = List(
      ColumnDefinition(BIGINT, ColumnName("verticaLong"), NotNullable, None, None),
      ColumnDefinition(DATE, ColumnName("verticaDate"), NotNullable, None, None),
      ColumnDefinition(VARCHAR, ColumnName("verticaVarchar1"), NotNullable, Some(1), None))
    assert(typeDescriptor.columnDefn.columns.toList === expectedColumns)

    // Vertica uses `Integer`
    val int64TypeNames = List("Integer", "INTEGER", "INT", "BIGINT", "INT8", "SMALLINT",
      "TINYINT", "SMALLINT", "MEDIUMINT")
    // Vertica uses `Date`
    val dateTypeNames = List("Date", "DATE")
    // Vertica uses `Varchar`
    val varcharTypeNames = List("Varchar", "VARCHAR")

    int64TypeNames foreach { int64TypeName =>
      dateTypeNames foreach { dateTypeName =>
        varcharTypeNames foreach { varcharTypeName =>
          val resultSetMetaData = mock[ResultSetMetaData]
          when(resultSetMetaData.getColumnTypeName(1)) thenReturn (int64TypeName)
          when(resultSetMetaData.isNullable(1)) thenReturn (ResultSetMetaData.columnNoNulls)
          when(resultSetMetaData.getColumnTypeName(2)) thenReturn (dateTypeName)
          when(resultSetMetaData.isNullable(2)) thenReturn (ResultSetMetaData.columnNoNulls)
          when(resultSetMetaData.getColumnTypeName(3)) thenReturn (varcharTypeName)
          when(resultSetMetaData.isNullable(3)) thenReturn (ResultSetMetaData.columnNoNulls)

          val validationResult =
            typeDescriptor.columnDefn.resultSetExtractor.validate(resultSetMetaData)

          assert(validationResult.isSuccess, validationResult)
        }
      }
    }
  }

  "Big Jdbc Test" should {

    isColumnDefinitionAvailable[ExhaustiveJdbcCaseClass]

    // explictly just call this to get a compiler error
    DBMacro.toDBTypeDescriptor[ExhaustiveJdbcCaseClass]
    // ensure the implicit fires
    isJDBCTypeInfoAvailable[ExhaustiveJdbcCaseClass]

    val expectedColumns = List(
      ColumnDefinition(BIGINT, ColumnName("bigInt"), NotNullable, None, None),
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

    val typeDesc = DBMacro.toDBTypeDescriptor[ExhaustiveJdbcCaseClass]
    val columnDef = typeDesc.columnDefn
    assert(columnDef.columns.toList === expectedColumns)

    val rsmd = mock[ResultSetMetaData]
    when(rsmd.getColumnTypeName(1)) thenReturn ("BIGINT")
    when(rsmd.isNullable(1)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(2)) thenReturn ("INT")
    when(rsmd.isNullable(2)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(3)) thenReturn ("INTEGER") // synonym of INT
    when(rsmd.isNullable(3)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(4)) thenReturn ("SMALLINT")
    when(rsmd.isNullable(4)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(5)) thenReturn ("DOUBLE")
    when(rsmd.isNullable(5)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(6)) thenReturn ("TINYINT")
    when(rsmd.isNullable(6)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(7)) thenReturn ("VARCHAR")
    when(rsmd.isNullable(7)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(8)) thenReturn ("CHAR") // synonym of VARCHAR
    when(rsmd.isNullable(8)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(9)) thenReturn ("TEXT")
    when(rsmd.isNullable(9)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(10)) thenReturn ("TEXT")
    when(rsmd.isNullable(10)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(11)) thenReturn ("VARCHAR")
    when(rsmd.isNullable(11)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(12)) thenReturn ("DATETIME")
    when(rsmd.isNullable(12)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(13)) thenReturn ("DATE")
    when(rsmd.isNullable(13)) thenReturn (ResultSetMetaData.columnNoNulls)
    when(rsmd.getColumnTypeName(14)) thenReturn ("BIGINT")
    when(rsmd.isNullable(14)) thenReturn (ResultSetMetaData.columnNullable)

    assert(columnDef.resultSetExtractor.validate(rsmd).isSuccess)

    val rs = mock[ResultSet]
    when(rs.getLong("bigInt")) thenReturn (12345678L)
    when(rs.getInt("smallerAgainInt")) thenReturn (123)
    when(rs.getInt("normalIntWithSize")) thenReturn (12)
    when(rs.getInt("evenSmallerInt")) thenReturn (1)
    when(rs.getDouble("numberFun")) thenReturn (1.1)
    when(rs.getBoolean("booleanFlag")) thenReturn (true)
    when(rs.getString("smallString")) thenReturn ("small_string")
    when(rs.getString("smallishString")) thenReturn ("smallish_string")
    when(rs.getString("largeString")) thenReturn ("large_string")
    when(rs.getString("forceTextString")) thenReturn ("force_text_string")
    when(rs.getString("forcedVarChar")) thenReturn ("forced_var_char")
    when(rs.getTimestamp("myDateWithTime")) thenReturn (new java.sql.Timestamp(1111L))
    when(rs.getTimestamp("myDateWithoutTime")) thenReturn (new java.sql.Timestamp(1112L))
    when(rs.getLong("optiLong")) thenReturn (1113L)

    assert(columnDef.resultSetExtractor.toCaseClass(rs, typeDesc.converter) ==
      ExhaustiveJdbcCaseClass(
        12345678L,
        123,
        12,
        1,
        1.1,
        true,
        "small_string",
        "smallish_string",
        "large_string",
        "force_text_string",
        "forced_var_char",
        new Date(1111L),
        new Date(1112L),
        Some(1113L)))
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
  }

  "TupleConverter for Date" should {
    val typeDesc = DBMacro.toDBTypeDescriptor[CaseClassWithDate]
    val converter = typeDesc.converter
    val date1 = new Date(100L)
    val date2 = new Date(200L)
    val t = Tuple.size(3)
    t.setLong(0, 99L)
    t.set(1, date1)
    t.set(2, date2)
    assert(CaseClassWithDate(99L, date1, date2) == converter(new TupleEntry(t)))
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
  }

  "ResultSetExtractor validation for nullable columns" should {

    val typeDesc = DBMacro.toDBTypeDescriptor[CaseClassWithOptions]
    val columnDef = typeDesc.columnDefn

    val rsmd = mock[ResultSetMetaData]
    when(rsmd.getColumnTypeName(1)) thenReturn ("INT")
    when(rsmd.isNullable(1)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(2)) thenReturn ("VARCHAR")
    when(rsmd.isNullable(2)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(3)) thenReturn ("DATETIME")
    when(rsmd.isNullable(3)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(4)) thenReturn ("BOOLEAN")
    when(rsmd.isNullable(4)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(5)) thenReturn ("SMALLINT")
    when(rsmd.isNullable(5)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(6)) thenReturn ("BIGINT")
    when(rsmd.isNullable(6)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(7)) thenReturn ("DOUBLE")
    when(rsmd.isNullable(7)) thenReturn (ResultSetMetaData.columnNullable)

    assert(columnDef.resultSetExtractor.validate(rsmd).isSuccess)
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
  }

  "ResultSetExtractor when nullable values are not null" should {
    val typeDesc = DBMacro.toDBTypeDescriptor[CaseClassWithOptions]
    val columnDef = typeDesc.columnDefn

    val rs = mock[ResultSet]
    when(rs.getInt("id")) thenReturn (26)
    when(rs.wasNull) thenReturn (false)
    when(rs.getString("name")) thenReturn ("alice")
    when(rs.wasNull) thenReturn (false)
    when(rs.getTimestamp("date_id")) thenReturn (new java.sql.Timestamp(1111L))
    when(rs.wasNull) thenReturn (false)
    when(rs.getBoolean("boolean_value")) thenReturn (true)
    when(rs.wasNull) thenReturn (false)
    when(rs.getInt("short_value")) thenReturn (2)
    when(rs.wasNull) thenReturn (false)
    when(rs.getLong("long_value")) thenReturn (2000L)
    when(rs.wasNull) thenReturn (false)
    when(rs.getDouble("double_value")) thenReturn (2.2)
    when(rs.wasNull) thenReturn (false)
    assert(columnDef.resultSetExtractor.toCaseClass(rs, typeDesc.converter) ==
      CaseClassWithOptions(Some(26), Some("alice"), Some(new Date(1111L)),
        Some(true), Some(2), Some(2000L), Some(2.2)))
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
  }

  "ResultSetExtractor when null values" should {
    val typeDesc = DBMacro.toDBTypeDescriptor[CaseClassWithOptions]
    val columnDef = typeDesc.columnDefn

    val rs = mock[ResultSet]
    when(rs.getInt("id")) thenReturn (0) // jdbc returns 0 for null numeric values
    when(rs.wasNull) thenReturn (true)
    when(rs.getString("name")) thenReturn (null)
    when(rs.wasNull) thenReturn (true)
    when(rs.getString("date_id")) thenReturn (null)
    when(rs.getBoolean("boolean_value")) thenReturn (false) // jdbc returns false for null boolean values
    when(rs.wasNull) thenReturn (true)
    when(rs.getInt("short_value")) thenReturn (0)
    when(rs.wasNull) thenReturn (true)
    when(rs.getLong("long_value")) thenReturn (0L)
    when(rs.wasNull) thenReturn (true)
    when(rs.getDouble("double_value")) thenReturn (0)
    when(rs.wasNull) thenReturn (true)
    assert(columnDef.resultSetExtractor.toCaseClass(rs, typeDesc.converter) ==
      CaseClassWithOptions(None, None, None,
        None, None, None, None))
    () // Need this till: https://github.com/scalatest/scalatest/issues/1107
  }

  "ResultSetExtractor for DB schema type mismatch" in {
    val typeDesc = DBMacro.toDBTypeDescriptor[CaseClassWithOptions]
    val columnDef = typeDesc.columnDefn

    val rsmd = mock[ResultSetMetaData]
    when(rsmd.getColumnTypeName(1)) thenReturn ("INT")
    when(rsmd.isNullable(1)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(2)) thenReturn ("TINYINT") // mismatch
    when(rsmd.isNullable(2)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(3)) thenReturn ("DATETIME")
    when(rsmd.isNullable(3)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(4)) thenReturn ("BOOLEAN")
    when(rsmd.isNullable(4)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(5)) thenReturn ("SMALLINT")
    when(rsmd.isNullable(5)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(6)) thenReturn ("BIGINT")
    when(rsmd.isNullable(6)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(7)) thenReturn ("DOUBLE")
    when(rsmd.isNullable(7)) thenReturn (ResultSetMetaData.columnNullable)

    assert(columnDef.resultSetExtractor.validate(rsmd).isFailure)
  }

  "ResultSetExtractor for DB schema nullable mismatch" in {
    val typeDesc = DBMacro.toDBTypeDescriptor[CaseClassWithOptions]
    val columnDef = typeDesc.columnDefn

    val rsmd = mock[ResultSetMetaData]
    when(rsmd.getColumnTypeName(1)) thenReturn ("INT")
    when(rsmd.isNullable(1)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(2)) thenReturn ("VARCHAR")
    when(rsmd.isNullable(2)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(3)) thenReturn ("DATETIME")
    when(rsmd.isNullable(3)) thenReturn (ResultSetMetaData.columnNoNulls) // mismatch
    when(rsmd.getColumnTypeName(4)) thenReturn ("BOOLEAN")
    when(rsmd.isNullable(4)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(5)) thenReturn ("SMALLINT")
    when(rsmd.isNullable(5)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(6)) thenReturn ("BIGINT")
    when(rsmd.isNullable(6)) thenReturn (ResultSetMetaData.columnNullable)
    when(rsmd.getColumnTypeName(7)) thenReturn ("DOUBLE")
    when(rsmd.isNullable(7)) thenReturn (ResultSetMetaData.columnNullable)

    assert(columnDef.resultSetExtractor.validate(rsmd).isFailure)
  }

  "Duplicate nested fields should be blocked" in {
    a[TestFailedException] should be thrownBy isColumnDefinitionAvailable[OuterWithBadNesting]
  }
}
