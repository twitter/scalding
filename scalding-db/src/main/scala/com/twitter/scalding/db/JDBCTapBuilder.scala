/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.db

import com.twitter.scalding.{ AccessMode, Hdfs, Mode, Source, TestTapFactory }
import cascading.jdbc.JDBCTap
import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.scalding.db.driver.JDBCDriver

/**
 * Builds a cascading-jdbc tap from the supplied column definitions and config options.
 */
object JDBCTapBuilder {
  def build(cols: Iterable[ColumnDefinition], jdbcOptions: JDBCOptions): JDBCTap =
    try {
      val ConnectionSpec(url, uName, passwd, jdbcDriverName, _) = jdbcOptions.currentConfig
      val jdbcDriver = JDBCDriver(jdbcDriverName)

      val tableDesc = jdbcDriver.getTableDesc(jdbcOptions.tableName, cols)

      val jdbcScheme = jdbcDriver.getJDBCScheme(cols.map(_.name),
        jdbcOptions.filterCondition,
        jdbcOptions.updateBy,
        jdbcOptions.replaceOnInsert)

      val tap = new JDBCTap(
        url.toStr,
        uName.toStr,
        passwd.toStr,
        jdbcDriver.driver.toStr,
        tableDesc,
        jdbcScheme)

      tap.setConcurrentReads(jdbcOptions.maxConcurrentReads)
      tap.setBatchSize(jdbcOptions.batchSize)
      tap
    } catch {
      case e: NullPointerException => sys.error("Could not find DB credential information.")
    }
}
