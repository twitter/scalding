/*
Copyright 2015 Twitter, Inc.

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

// Schema name in database, used for vertica currently
case class SchemaName(toStr: String) extends AnyVal
// Table name in database
case class TableName(toStr: String) extends AnyVal
// Jdbc style connection url, e.g.: "jdbc:mysql://mysql01.company.com:3306/production"
case class ConnectUrl(toStr: String) extends AnyVal
// Username for the database
case class UserName(toStr: String) extends AnyVal
// Password for the database
case class Password(toStr: String) extends AnyVal {
  override def toString: String = super.toString
}
// The adapter to use
case class Adapter(toStr: String) extends AnyVal
// Hadoop path string. Can be absolute path or complete URI.
case class HadoopUri(toStr: String) extends AnyVal
// Sql query string
case class SqlQuery(toStr: String) extends AnyVal
// java.nio.charset types are not serializable, so we define our own
case class StringEncoding(toStr: String) extends AnyVal

/**
 * Pass your DB credentials to this class in a preferred secure way
 */
case class ConnectionConfig(
  connectUrl: ConnectUrl,
  userName: UserName,
  password: Password,
  adapter: Adapter,
  encoding: StringEncoding)

case class Database(toStr: String) extends AnyVal

case class AvailableDatabases(m: Map[Database, ConnectionConfig] = Map()) {
  def get(d: Database) = m.get(d)
  def apply(d: Database) = m.apply(d)
}
