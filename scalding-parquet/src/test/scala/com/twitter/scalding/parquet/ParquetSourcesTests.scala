package com.twitter.scalding.parquet

import cascading.tuple.Fields
import com.twitter.scalding.parquet.thrift.{ FixedPathParquetThrift, HourlySuffixParquetThrift, DailySuffixParquetThrift }
import com.twitter.scalding.parquet.tuple.{ HourlySuffixParquetTuple, FixedPathParquetTuple, DailySuffixParquetTuple }
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

  testSource(new DailySuffixParquetThrift[MockTBase](path, dateRange))
  testSource(new HourlySuffixParquetThrift[MockTBase](path, dateRange))
  testSource(new FixedPathParquetThrift[MockTBase](path, path, path))

  testSource(new DailySuffixParquetTuple(path, dateRange, fields))
  testSource(new HourlySuffixParquetTuple(path, dateRange, fields))
  testSource(new FixedPathParquetTuple(fields, path, path, path))

  def testSource[S <: Source with HasFilterPredicate[S]](src: S) = {
    src.getClass.getSimpleName should {

      // use an explicit type here to
      // further ensure that withFilter returns something of type S
      // (calling withFilter does not give you just a HasFilterPredicate, but a Source
      // of the same type as what you called withFilter on
      val withFilter1: S = src.withFilter(filter1)
      val withFilter1And2: S = withFilter1.withFilter(filter2)

      "default to no filter predicate" in {
        src.filterPredicate must be equalTo None
      }

      "make immutable copies" in {
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