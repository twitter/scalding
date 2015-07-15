package com.twitter.scalding.parquet.scrooge

import com.twitter.scalding.parquet.{ StrictColumnProjectionString, DeprecatedColumnProjectionString, ParquetSourcesTestsBase }
import com.twitter.scrooge.ThriftStruct
import org.apache.thrift.protocol.TProtocol
import org.apache.parquet.filter2.predicate.FilterPredicate

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
      }, DeprecatedColumnProjectionString(columnStrings))

    testReturnProvidedColumns(
      new DailySuffixParquetScrooge[MockThriftStruct](path, dateRange) {
        override def withColumnProjections: Set[String] = columnStrings
      }, StrictColumnProjectionString(columnStrings))

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
      }, DeprecatedColumnProjectionString(columnStrings))

    testReturnProvidedColumns(
      new HourlySuffixParquetScrooge[MockThriftStruct](path, dateRange) {
        override def withColumnProjections: Set[String] = columnStrings
      }, StrictColumnProjectionString(columnStrings))

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
      }, DeprecatedColumnProjectionString(columnStrings))

    testReturnProvidedColumns(
      new FixedPathParquetScrooge[MockThriftStruct](path, path, path) {
        override def withColumnProjections: Set[String] = columnStrings
      }, StrictColumnProjectionString(columnStrings))

  }

}

class MockThriftStruct extends ThriftStruct {
  override def write(oprot: TProtocol): Unit = ()
}