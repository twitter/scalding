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

package com.twitter.scalding_internal.db.jdbc

import com.twitter.scalding_internal.db._
// Generic options/extensions for accessing all JDBC sources
trait JDBCOptions {
  // Name of the table in the database
  val tableName: TableName
  // Current database
  val database: Database

  def availableDatabases: AvailableDatabases

  // Connection options
  def connectionConfig: ConnectionConfig = availableDatabases(database)

  // Must be a subset of column names.
  // If updateBy column names are given, a SQL UPDATE statement will be generated
  // if the values in those columns for the given Tuple are all not {@code null}.
  // Otherwise an INSERT statement will be generated.
  val updateBy: Iterable[String] = Nil

  // The body of a WHERE clause. If present will filter the full table by this condition.
  val filterCondition: Option[String] = None

  // Override this if your table is really large
  def maxConcurrentReads = 1

  // How many rows to insert/update into this table in a batch?
  def batchSize = 1000

  // If true, will perform an update when inserting a row with a primary or unique key that already
  // exists in the table. Will replace the old values in that row with the new values.
  val replaceOnInsert: Boolean = false
}

trait JDBCLoadOptions {
  // optional query to execute before load
  def preloadQuery: Option[SqlQuery] = None

  // optional query to execute after load
  def postloadQuery: Option[SqlQuery] = None
}
