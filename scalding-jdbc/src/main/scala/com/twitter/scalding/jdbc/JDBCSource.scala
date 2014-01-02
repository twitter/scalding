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

package com.twitter.scalding.jdbc

import com.twitter.scalding.{ AccessMode, Hdfs, Mode, Source, TestTapFactory }
import cascading.jdbc.JDBCScheme
import cascading.jdbc.JDBCTap
import cascading.jdbc.TableDesc
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.Fields

/**
 * Extend this source to let scalding read from or write to a database.
 * In order for this to work you need to specify the table name, column definitions and DB credentials.
 * If you write to a DB, the fields in the final pipe have to correspond to the column names in the DB table.
 * Example usage:
 * case object YourTableSource extends JDBCSource {
 *   override val tableName = "tableName"
 *   override val columns = List(
 *      varchar("col1", 64),
 *      date("col2"),
 *      tinyint("col3"),
 *      double("col4"),
 *   )
 *   override def currentConfig = ConnectionSpec("www.github.com", "username", "password", "mysql")
 * }
 *
 * @author Argyris Zymnis
 * @author Oscar Boykin
 * @author Kevin Lin
 */
abstract class JDBCSource extends Source {

  // Override the following three members when you extend this class
  val tableName : String
  val columns : Iterable[ColumnDefinition]
  protected def currentConfig : ConnectionSpec

  // Must be a subset of column names.
  // If updateBy column names are given, a SQL UPDATE statement will be generated
  // if the values in those columns for the given Tuple are all not {@code null}.
  // Otherwise an INSERT statement will be generated.
  val updateBy : Iterable[String] = Nil

  // The body of a WHERE clause. If present will filter the full table by this condition.
  val filterCondition: Option[String] = None

  // Override this if your table is really large
  def maxConcurrentReads = 1

  // How many rows to insert/update into this table in a batch?
  def batchSize = 1000

  protected def driverFor(adapter: String): String = 
    Map("mysql" -> "com.mysql.jdbc.Driver",
        "hsqldb" -> "org.hsqldb.jdbcDriver")
    .apply(adapter)

  def fields : Fields = new Fields(columnNames.toSeq :_*)

  protected def columnNames : Array[String] = columns.map{ _.name }.toArray
  protected def columnDefinitions : Array[String] = columns.map{ _.definition }.toArray
  protected def tableDesc = new TableDesc(tableName, columnNames, columnDefinitions, null)

  protected def nullStr(nullable : Boolean) = if(nullable) "NULL" else "NOT NULL"

  protected def mkColumnDef(name : String, typeName : String,
    nullable : Boolean, sizeOp : Option[Int] = None, defOp : Option[String]) = {
    val sizeStr = sizeOp.map { "(" + _.toString + ")" }.getOrElse("")
    val defStr = defOp.map { " DEFAULT '" + _.toString + "' " }.getOrElse(" ")
    ColumnDefinition(name, typeName + sizeStr + defStr + nullStr(nullable))
  }

  // Some helper methods that we can use to generate column definitions
  protected def bigint(name : String, size : Int = 20, nullable : Boolean = false) = {
    mkColumnDef(name, "BIGINT", nullable, Some(size), None)
  }

  protected def int(name : String, size : Int = 11, defaultValue : Int = 0, nullable : Boolean = false) = {
    mkColumnDef(name, "INT", nullable, Some(size), Some(defaultValue.toString))
  }

  protected def smallint(name : String, size : Int = 6, defaultValue : Int = 0, nullable : Boolean = false) = {
    mkColumnDef(name, "SMALLINT", nullable, Some(size), Some(defaultValue.toString))
  }

  // NOTE: tinyint(1) actually gets converted to a java Boolean
  protected def tinyint(name : String, size : Int = 8, nullable : Boolean = false) = {
    mkColumnDef(name, "TINYINT", nullable, Some(size), None)
  }

  protected def varchar(name : String, size : Int = 255, nullable : Boolean = false) = {
    mkColumnDef(name, "VARCHAR", nullable, Some(size), None)
  }

  protected def date(name : String, nullable : Boolean = false) = {
    mkColumnDef(name, "DATE", nullable, None, None)
  }

  protected def datetime(name : String, nullable : Boolean = false) = {
    mkColumnDef(name, "DATETIME", nullable, None, None)
  }

  protected def text(name : String, nullable : Boolean = false) = {
    mkColumnDef(name, "TEXT", nullable, None, None)
  }

  protected def double(name : String, nullable : Boolean = false) = {
    mkColumnDef(name, "DOUBLE", nullable, None, None)
  }

  protected def column(name : String, definition : String) = ColumnDefinition(name, definition)

  protected def createJDBCTap = {
    try {
      val ConnectionSpec(url, uName, passwd, adapter) = currentConfig
      val tap = new JDBCTap(url, uName, passwd, driverFor(adapter), tableDesc, getJDBCScheme)
      tap.setConcurrentReads(maxConcurrentReads)
      tap.setBatchSize(batchSize)
      tap
    } catch {
      case e: NullPointerException => {
        sys.error("Could not find DB credential information.")
      } 
    }
  }

  protected def getJDBCScheme = new JDBCScheme(
    null,  // inputFormatClass
    null,  // outputFormatClass
    columnNames.toArray,
    null,  // orderBy
    filterCondition.getOrElse(null),
    updateBy.toArray
  )

  override def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] = {
    mode match {
      case Hdfs(_,_) => createJDBCTap.asInstanceOf[Tap[_,_,_]]
      // TODO: support Local mode here, and better testing.
      case _ => TestTapFactory(this, fields).createTap(readOrWrite)
    }
  }
   
  // Generate SQL statement to create the DB table if not existing.
  def toSqlCreateString : String = {
    def addBackTicks(str : String) = "`" + str + "`"
    val allCols = columns
      .map { cd => addBackTicks(cd.name) + " " + cd.definition }
      .mkString(",\n")

    "CREATE TABLE " + addBackTicks(tableName) + " (\n" + allCols + ",\n PRIMARY KEY HERE!!!!"
  }
}

case class ColumnDefinition(name : String, definition : String)
/**
* Pass your DB credentials to this class in a preferred secure way
*/
case class ConnectionSpec(connectUrl : String, userName : String, password : String, adapter : String)
