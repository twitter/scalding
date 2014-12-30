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

package com.twitter.scalding.jdbc.driver

import com.twitter.scalding.jdbc._

import cascading.jdbc.{ MySqlScheme, JDBCScheme, TableDesc }

private[jdbc] abstract class VerticaBase() extends JDBCDriver {
  protected override def columnMutator: PartialFunction[DriverColumnDefinition, DriverColumnDefinition] = {
    case t @ DriverColumnDefinition(BIGINT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DriverColumnDefinition(INT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DriverColumnDefinition(SMALLINT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DriverColumnDefinition(TINYINT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DriverColumnDefinition(DOUBLE, _, _, _, _, _) => t.copy(sqlType = SqlTypeName("DOUBLE PRECISION"))
  }
}

/**
 * Old Vertica 4.1 jdbc driver
 * see https://my.vertica.com/docs/5.1.6/HTML/index.htm#16699.htm
 */
case class OldVerticaDriver() extends VerticaBase {
  override val driver = DriverClass("com.vertica.Driver")
}

/**
 * Vertica jdbc driver (5.1 and higher)
 */
case class VerticaDriver() extends VerticaBase {
  override val driver = DriverClass("com.vertica.jdbc.Driver")
}

