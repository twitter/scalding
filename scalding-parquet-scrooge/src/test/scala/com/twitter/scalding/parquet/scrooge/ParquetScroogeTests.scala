package com.twitter.scalding.parquet.scrooge

import com.twitter.scalding.parquet.ParquetSourcesTestsBase
import com.twitter.scrooge.ThriftStruct
import org.apache.thrift.protocol.TProtocol
import parquet.filter2.predicate.FilterPredicate

class ParquetScroogeTests extends ParquetSourcesTestsBase {

  "DailySuffixParquetScrooge" should {
    val default = new DailySuffixParquetScrooge[MockThriftStruct](path, dateRange)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new DailySuffixParquetScrooge[MockThriftStruct](path, dateRange) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })

    testDefaultColumns(default)

    testReturnProvidedColumns(
      new DailySuffixParquetScrooge[MockThriftStruct](path, dateRange) {
        override def withColumns: Set[String] = columnStrings
      })
  }

  "HourlySuffixParquetScrooge" should {
    val default = new HourlySuffixParquetScrooge[MockThriftStruct](path, dateRange)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new HourlySuffixParquetScrooge[MockThriftStruct](path, dateRange) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })

    testDefaultColumns(default)

    testReturnProvidedColumns(
      new HourlySuffixParquetScrooge[MockThriftStruct](path, dateRange) {
        override def withColumns: Set[String] = columnStrings
      })
  }

  "FixedPathParquetScrooge" should {
    val default = new FixedPathParquetScrooge[MockThriftStruct](path, path, path)

    testDefaultFilter(default)

    testReturnProvidedFilter(
      new FixedPathParquetScrooge[MockThriftStruct](path, path, path) {
        override val withFilter: Option[FilterPredicate] = Some(filter1)
      })

    testDefaultColumns(default)

    testReturnProvidedColumns(
      new FixedPathParquetScrooge[MockThriftStruct](path, path, path) {
        override def withColumns: Set[String] = columnStrings
      })
  }

}

class MockThriftStruct extends ThriftStruct {
  override def write(oprot: TProtocol): Unit = ()
}