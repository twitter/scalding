package com.twitter.scalding.parquet

import cascading.tuple.Fields
import com.twitter.scalding.parquet.thrift.{ DailySuffixParquetThrift, FixedPathParquetThrift, HourlySuffixParquetThrift }
import com.twitter.scalding.parquet.tuple.{ DailySuffixParquetTuple, FixedPathParquetTuple, HourlySuffixParquetTuple }
import com.twitter.scalding.{ DateRange, RichDate, Source }
import java.lang.{ Integer => JInt }
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.{ TBase, TFieldIdEnum }
import org.scalatest.WordSpec
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.{ FilterApi, FilterPredicate }

abstract class ParquetSourcesTestsBase extends WordSpec {

  val dateRange = DateRange(RichDate(0L), RichDate(0L))
  val path = "/a/path"
  val filter1: FilterPredicate = FilterApi.eq(intColumn("foo"), new JInt(7))
  val fields = new Fields("foo", "bar")
  val columnStrings = Set("a", "b", "c")

  def testDefaultFilter[S <: Source with HasFilterPredicate](src: S) = {
    "default to no filter predicate" in {
      assert(src.withFilter === None)
    }
  }

  def testReturnProvidedFilter[S <: Source with HasFilterPredicate](src: S) = {
    "return the provided filter" in {
      assert(src.withFilter === Some(filter1))
    }
  }

  def testDefaultColumns[S <: Source with HasColumnProjection](src: S) = {
    "default to no column projection" in {
      assert(src.columnProjectionString === None)
      assert(src.withColumns === Set())
      assert(src.withColumnProjections === Set())
    }
  }

  def testReturnProvidedColumns[S <: Source with HasColumnProjection](src: S, expected: ColumnProjectionString) = {
    "return the provided columns " + expected in {
      assert(src.columnProjectionString.get === expected)
    }

    "correctly format globs into parquet's expected format " + expected in {
      verifyParquetStringFormat(src.columnProjectionString.get.asSemicolonString, expected.globStrings)
    }
  }

  private def verifyParquetStringFormat(s: String, expected: Set[String]) = {
    assert(s.split(";").toSet === expected)
  }
}

class ParquetSourcesTests extends ParquetSourcesTestsBase {

  "DailySuffixParquetThrift" should {
    val default = new DailySuffixParquetThrift[MockTBase](path, dateRange)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new DailySuffixParquetThrift[MockTBase](path, dateRange) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })

    testDefaultColumns(default)

    testReturnProvidedColumns(
      new DailySuffixParquetThrift[MockTBase](path, dateRange) {
        override def withColumns: Set[String] = columnStrings
      }, DeprecatedColumnProjectionString(columnStrings))

    testReturnProvidedColumns(
      new DailySuffixParquetThrift[MockTBase](path, dateRange) {
        override def withColumnProjections: Set[String] = columnStrings
      }, StrictColumnProjectionString(columnStrings))

  }

  "HourlySuffixParquetThrift" should {
    val default = new HourlySuffixParquetThrift[MockTBase](path, dateRange)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new HourlySuffixParquetThrift[MockTBase](path, dateRange) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })

    testDefaultColumns(default)

    testReturnProvidedColumns(
      new HourlySuffixParquetThrift[MockTBase](path, dateRange) {
        override def withColumns: Set[String] = columnStrings
      }, DeprecatedColumnProjectionString(columnStrings))

    testReturnProvidedColumns(
      new HourlySuffixParquetThrift[MockTBase](path, dateRange) {
        override def withColumnProjections: Set[String] = columnStrings
      }, StrictColumnProjectionString(columnStrings))

  }

  "FixedPathParquetThrift" should {
    val default = new FixedPathParquetThrift[MockTBase](path, path, path)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new FixedPathParquetThrift[MockTBase](path, path, path) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })

    testDefaultColumns(default)

    testReturnProvidedColumns(
      new FixedPathParquetThrift[MockTBase](path, path, path) {
        override def withColumns: Set[String] = columnStrings
      }, DeprecatedColumnProjectionString(columnStrings))

    testReturnProvidedColumns(
      new FixedPathParquetThrift[MockTBase](path, path, path) {
        override def withColumnProjections: Set[String] = columnStrings
      }, StrictColumnProjectionString(columnStrings))

  }

  "DailySuffixParquetTuple" should {
    val default = new DailySuffixParquetTuple(path, dateRange, fields)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new DailySuffixParquetTuple(path, dateRange, fields) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })
  }

  "HourlySuffixParquetTuple" should {
    val default = new HourlySuffixParquetTuple(path, dateRange, fields)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new HourlySuffixParquetTuple(path, dateRange, fields) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })
  }

  "FixedPathParquetTuple" should {
    val default = new FixedPathParquetTuple(fields, path, path, path)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new FixedPathParquetTuple(fields, path, path, path) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })
  }
}

class MockTBase extends TBase[MockTBase, TFieldIdEnum] {
  override def read(p1: TProtocol): Unit = ()
  override def write(p1: TProtocol): Unit = ()
  override def fieldForId(p1: Int): TFieldIdEnum = null
  override def isSet(p1: TFieldIdEnum): Boolean = false
  override def getFieldValue(p1: TFieldIdEnum): AnyRef = null
  override def setFieldValue(p1: TFieldIdEnum, p2: scala.Any): Unit = ()
  override def deepCopy(): MockTBase = null
  override def clear(): Unit = ()
  override def compareTo(o: MockTBase): Int = 0
}
