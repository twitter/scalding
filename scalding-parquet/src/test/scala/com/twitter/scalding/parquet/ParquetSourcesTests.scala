package com.twitter.scalding.parquet

import cascading.tuple.Fields
import com.twitter.scalding.parquet.thrift.{ DailySuffixParquetThrift, FixedPathParquetThrift, HourlySuffixParquetThrift }
import com.twitter.scalding.parquet.tuple.{ DailySuffixParquetTuple, FixedPathParquetTuple, HourlySuffixParquetTuple }
import com.twitter.scalding.{ DateRange, RichDate, Source }
import java.lang.{ Integer => JInt }
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.{ TBase, TFieldIdEnum }
import org.specs.Specification
import parquet.filter2.predicate.FilterApi._
import parquet.filter2.predicate.{ FilterApi, FilterPredicate }

class ParquetSourcesTests extends Specification {

  val dateRange = DateRange(RichDate(0L), RichDate(0L))
  val path = "/a/path"
  val filter1: FilterPredicate = FilterApi.eq(intColumn("foo"), new JInt(7))
  val filter2: FilterPredicate = FilterApi.eq(intColumn("bar"), new JInt(77))
  val fields = new Fields("foo", "bar")

  testParquetThriftSource(new DailySuffixParquetThrift[MockTBase](path, dateRange))
  testParquetThriftSource(new HourlySuffixParquetThrift[MockTBase](path, dateRange))
  testParquetThriftSource(new FixedPathParquetThrift[MockTBase](path, path, path))

  testParquetTupleSource(new DailySuffixParquetTuple(path, dateRange, fields))
  testParquetTupleSource(new HourlySuffixParquetTuple(path, dateRange, fields))
  testParquetTupleSource(new FixedPathParquetTuple(fields, path, path, path))

  def testParquetThriftSource[S <: Source with HasFilterPredicate[S] with HasColumnProjection[S]](src: S) = {
    src.getClass.getSimpleName should {
      testFilterPredicates(src)
      testColumnProjection(src)

      "copy filters when using column projection" in {
        val both: S = src.withFilter(filter1).withColumns("foo")
        both.filterPredicate must be equalTo Some(filter1)
        both.columnGlobs must be equalTo Set(ColumnProjectionGlob("foo"))
      }

      "copy columns when using filters" in {
        val both: S = src.withColumns("foo").withFilter(filter1)
        both.filterPredicate must be equalTo Some(filter1)
        both.columnGlobs must be equalTo Set(ColumnProjectionGlob("foo"))
      }

    }
  }

  def testParquetTupleSource[S <: Source with HasFilterPredicate[S]](src: S) = {
    src.getClass.getSimpleName should {
      testFilterPredicates(src)
    }
  }

  def testFilterPredicates[S <: Source with HasFilterPredicate[S]](src: S) = {
    // use an explicit type here to
    // further ensure that withFilter returns something of type S
    // (calling withFilter does not give you just a HasFilterPredicate, but a Source
    // of the same type as what you called withFilter on
    val withFilter1: S = src.withFilter(filter1)
    val withFilter1And2: S = withFilter1.withFilter(filter2)

    "default to no filter predicate" in {
      src.filterPredicate must be equalTo None
    }

    "make immutable copies when withFilter is called" in {
      withFilter1 mustNotEq src
      withFilter1And2 mustNotEq src
      withFilter1 mustNotEq withFilter1And2
    }

    "return the provided filter" in {
      withFilter1.filterPredicate must be equalTo Some(filter1)
    }

    "chain multiple filters together" in {
      withFilter1And2.filterPredicate must be equalTo Some(and(filter1, filter2))
    }
  }

  def testColumnProjection[S <: Source with HasColumnProjection[S]](src: S) = {

    val withColumn1: S = src.withColumns("foo/bar")
    val withColumn2: S = withColumn1.withColumns("foo/baz")
    val withColumn3: S = src.withColumns("a", "b", "c")

    "default to no column projection" in {
      src.columnGlobs must beEmpty
    }

    "make immutable copies when withColumns is called" in {
      src mustNotEq withColumn1
      src mustNotEq withColumn2
      src mustNotEq withColumn3
      withColumn1 mustNotEq withColumn2
      withColumn1 mustNotEq withColumn3
      withColumn2 mustNotEq withColumn3
    }

    "return the provided columns" in {
      withColumn1.columnGlobs must be equalTo Set(ColumnProjectionGlob("foo/bar"))
      withColumn3.columnGlobs must be equalTo Set(
        ColumnProjectionGlob("a"),
        ColumnProjectionGlob("b"),
        ColumnProjectionGlob("c"))
    }

    "chain multiple columsn together" in {
      withColumn2.columnGlobs must be equalTo Set(ColumnProjectionGlob("foo/bar"), ColumnProjectionGlob("foo/baz"))
    }

    "correctly format globs into parquet's expected format" in {
      src.globsInParquetStringFormat must be equalTo None
      verifyParquetStringFormat(withColumn1.globsInParquetStringFormat.get, Set("foo/bar"))
      verifyParquetStringFormat(withColumn2.globsInParquetStringFormat.get, Set("foo/bar", "foo/baz"))
      verifyParquetStringFormat(withColumn3.globsInParquetStringFormat.get, Set("a", "b", "c"))
    }

  }

  "ColumnProjectionGlob" should {
    "Not allow globs that contain ;" in {
      ColumnProjectionGlob("foo;baz") must throwAn[IllegalArgumentException]
    }
  }

  private def verifyParquetStringFormat(s: String, expected: Set[String]) = {
    s.split(";").toSet must be equalTo expected
  }
}

class MockTBase extends TBase[MockTBase, TFieldIdEnum] {
  override def read(p1: TProtocol): Unit = ()
  override def write(p1: TProtocol): Unit = ()
  override def fieldForId(p1: Int): TFieldIdEnum = null
  override def isSet(p1: TFieldIdEnum): Boolean = false
  override def getFieldValue(p1: TFieldIdEnum): AnyRef = null
  override def setFieldValue(p1: TFieldIdEnum, p2: scala.Any): Unit = ()
  override def deepCopy(): TBase[MockTBase, TFieldIdEnum] = null
  override def clear(): Unit = ()
  override def compareTo(o: MockTBase): Int = 0
}