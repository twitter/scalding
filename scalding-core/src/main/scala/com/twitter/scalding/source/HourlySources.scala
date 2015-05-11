package com.twitter.scalding.source

import com.twitter.scalding.{ DateRange, DateOps }
import org.apache.spark.rdd.RDD

abstract class HourlySuffixSource[T, M <: RDD[T]](prefixTemplate: String, dateRange: DateRange)
  extends TimePathedSource[T, M](prefixTemplate + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", dateRange, DateOps.UTC)

abstract class HourlySuffixMostRecentSource[T, M <: RDD[T]](prefixTemplate: String, dateRange: DateRange)
  extends MostRecentGoodSource[T, M](prefixTemplate + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", dateRange, DateOps.UTC)
