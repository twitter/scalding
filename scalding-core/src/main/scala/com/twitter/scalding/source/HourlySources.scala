/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.scalding.source

import com.twitter.scalding._

abstract class HourlySuffixSource(prefixTemplate: String, dateRange: DateRange)
  extends TimePathedSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", dateRange, DateOps.UTC)

abstract class HourlySuffixMostRecentSource(prefixTemplate: String, dateRange: DateRange)
  extends MostRecentGoodSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", dateRange, DateOps.UTC)

object HourlySuffixTsv {
  def apply(prefix: String)(implicit dateRange: DateRange) = new HourlySuffixTsv(prefix)
}

class HourlySuffixTsv(prefix: String)(override implicit val dateRange: DateRange)
  extends HourlySuffixSource(prefix, dateRange) with DelimitedScheme

object HourlySuffixTypedTsv {
  def apply[T](prefix: String)(implicit dateRange: DateRange, mf: Manifest[T], conv: TupleConverter[T], tset: TupleSetter[T]) =
    new HourlySuffixTypedTsv[T](prefix)
}

class HourlySuffixTypedTsv[T](prefix: String)(implicit override val dateRange: DateRange, override val mf: Manifest[T], override val conv: TupleConverter[T],
  override val tset: TupleSetter[T])
  extends HourlySuffixSource(prefix, dateRange) with TypedDelimited[T]

object HourlySuffixCsv {
  def apply(prefix: String)(implicit dateRange: DateRange) = new HourlySuffixCsv(prefix)
}

class HourlySuffixCsv(prefix: String)(override implicit val dateRange: DateRange)
  extends HourlySuffixSource(prefix, dateRange) with DelimitedScheme {
  override val separator = ","
}
