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
import cascading.jdbc.{ JDBCScheme, JDBCTap, MySqlScheme, TableDesc }
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.Fields

/**
 * Extend this source to let scalding read from or write to a database.
 * In order for this to work you need to specify the table name, column definitions and DB credentials.
 * If you write to a DB, the fields in the final pipe have to correspond to the column names in the DB table.
 * Example usage:
 * case object YourTableSource extends JDBCSource {
 *   override val tableName = TableName("tableName")
 *   override val columns = List(
 *      varchar("col1", 64),
 *      date("col2"),
 *      tinyint("col3"),
 *      double("col4")
 *   )
 *   override def currentConfig = ConnectionSpec(
 *     ConnectUrl("jdbc:mysql://mysql01.company.com:3306/production"),
 *     UserName("username"), Password("password"),
 *     MysqlDriver
 *   )
 * }
 *
 * @author Argyris Zymnis
 * @author Oscar Boykin
 * @author Kevin Lin
 */
abstract class JDBCSource[T <: JdbcDriver] extends Source {

  // Override the following three members when you extend this class
  val tableName: TableName
  val columns: Iterable[ColumnDefinition]
  protected def currentConfig: ConnectionSpec[T]

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

  def fields: Fields = new Fields(columnNames.map(_.get).toSeq: _*)

  protected def columnNames: Array[ColumnName] = columns.map(_.name).toArray
  protected def columnDefinitions: Array[Definition] = columns.map(_.definition).toArray

  // Some helper methods that we can use to generate column definitions
  protected def bigint(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, BIGINT.type]) = coldef(name, nullable)
  protected def int(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, INT.type]) = coldef(name, nullable)
  protected def smallint(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, SMALLINT.type]) = coldef(name, nullable)
  protected def tinyint(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, TINYINT.type]) = coldef(name, nullable)
  protected def varchar(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, VARCHAR.type]) = coldef(name, nullable)
  protected def date(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, DATE.type]) = coldef(name, nullable)
  protected def datetime(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, DATETIME.type]) = coldef(name, nullable)
  protected def text(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, TEXT.type]) = coldef(name, nullable)
  protected def double(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[T, DOUBLE.type]) = coldef(name, nullable)

  protected def createJDBCTap =
    try {
      val ConnectionSpec(url, uName, passwd, driver) = currentConfig
      val tap = new JDBCTap(
        url.get,
        uName.get,
        passwd.get,
        driver.driver.get,
        driver.getTableDesc(tableName, columnNames, columnDefinitions),
        getJDBCScheme(driver))
      tap.setConcurrentReads(maxConcurrentReads)
      tap.setBatchSize(batchSize)
      tap
    } catch {
      case e: NullPointerException => sys.error("Could not find DB credential information.")
    }

  protected def getJDBCScheme(driver: JdbcDriver) = driver match {
    case MysqlDriver =>
      new MySqlScheme(
        null, // inputFormatClass
        columnNames.map(_.get),
        null, // orderBy
        filterCondition.getOrElse(null),
        updateBy.toArray,
        replaceOnInsert)
    case _ => {
      if (replaceOnInsert) sys.error("replaceOnInsert functionality only supported by MySql")
      new JDBCScheme(
        null, // inputFormatClass
        null, // outputFormatClass
        columnNames.map(_.get),
        null, // orderBy
        filterCondition.getOrElse(null),
        updateBy.toArray)
    }
  }

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    mode match {
      case Hdfs(_, _) => createJDBCTap.asInstanceOf[Tap[_, _, _]]
      // TODO: support Local mode here, and better testing.
      case _ => TestTapFactory(this, fields).createTap(readOrWrite)
    }

  // Generate SQL statement to create the DB table if not existing.
  def toSqlCreateString: String = {
    def addBackTicks(str: String) = "`" + str + "`"
    val allCols = columns
      .map { case ColumnDefinition(ColumnName(name), Definition(defn)) => addBackTicks(name) + " " + defn }
      .mkString(",\n")

    "CREATE TABLE " + addBackTicks(tableName.get) + " (\n" + allCols + ",\n PRIMARY KEY HERE!!!!"
  }
}

case class ColumnName(get: String)
case class Definition(get: String)

case class ColumnDefinition(name: ColumnName, definition: Definition)

case class ConnectUrl(get: String)
case class UserName(get: String)
case class Password(get: String)

/**
 * Pass your DB credentials to this class in a preferred secure way
 */
case class ConnectionSpec[T <: JdbcDriver](connectUrl: ConnectUrl, userName: UserName, password: Password, adapter: T)

object JdbcDriver {
  def apply(str: String) = str.toLowerCase match {
    case "mysql" => MysqlDriver
    case "hsqldb" => HsqlDbDriver
    case "vertica" => VerticaDriver
    case _ => throw new IllegalArgumentException("Bad driver argument given: " + str)
  }
}

case class DriverClass(get: String)
case class TableName(get: String)

sealed trait JdbcDriver {
  def driver: DriverClass
  def getTableDesc(tableName: TableName, columnNames: Array[ColumnName], columnDefinitions: Array[Definition]) =
    new TableDesc(tableName.get, columnNames.map(_.get), columnDefinitions.map(_.get), null, null)
}

sealed trait JdbcType
case object BIGINT extends JdbcType
case object INT extends JdbcType
case object SMALLINT extends JdbcType
case object TINYINT extends JdbcType
case object BOOLEAN extends JdbcType
case object VARCHAR extends JdbcType
case object DATE extends JdbcType
case object DATETIME extends JdbcType
case object TEXT extends JdbcType
case object DOUBLE extends JdbcType

object IsNullable {
  def apply(isNullable: Boolean): IsNullable = if (isNullable) Nullable else NotNullable
}
sealed abstract class IsNullable(val get: String)
case object Nullable extends IsNullable("NULL")
case object NotNullable extends IsNullable("NOT NULL")

/**
 * This is a mechanism by which different databases can control and configure the way in which statements are created.
 */
trait DriverColumnDefiner[Driver <: JdbcDriver, Type <: JdbcType] {
  //TODO does this need to deal with sizes, or now that it's fixed per DB will that be fine?
  def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition
}

trait MysqlTableCreationImplicits {
  //TODO should use the fact that now we have more typed typeName
  private[this] def mkColumnDef(
    name: String,
    typeName: String,
    nullable: IsNullable,
    sizeOp: Option[Int] = None,
    defOp: Option[String]) = {
    val sizeStr = sizeOp.map { "(" + _.toString + ")" }.getOrElse("")
    val defStr = defOp.map { " DEFAULT '" + _.toString + "' " }.getOrElse(" ")
    ColumnDefinition(ColumnName(name), Definition(typeName + sizeStr + defStr + nullable.get))
  }

  implicit val bigint: DriverColumnDefiner[MysqlDriver.type, BIGINT.type] =
    new DriverColumnDefiner[MysqlDriver.type, BIGINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "BIGINT", nullable, Some(20), None)
    }

  implicit val int: DriverColumnDefiner[MysqlDriver.type, INT.type] =
    new DriverColumnDefiner[MysqlDriver.type, INT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "INT", nullable, Some(11), None)
    }

  implicit val smallint: DriverColumnDefiner[MysqlDriver.type, SMALLINT.type] =
    new DriverColumnDefiner[MysqlDriver.type, SMALLINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "SMALLINT", nullable, Some(6), Some("0"))
    }

  // NOTE: tinyint(1) actually gets converted to a java Boolean
  implicit val tinyint: DriverColumnDefiner[MysqlDriver.type, TINYINT.type] =
    new DriverColumnDefiner[MysqlDriver.type, TINYINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "TINYINT", nullable, Some(6), None)
    }

  implicit val varchar: DriverColumnDefiner[MysqlDriver.type, VARCHAR.type] =
    new DriverColumnDefiner[MysqlDriver.type, VARCHAR.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "VARCHAR", nullable, Some(255), None)
    }

  implicit val date: DriverColumnDefiner[MysqlDriver.type, DATE.type] =
    new DriverColumnDefiner[MysqlDriver.type, DATE.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DATE", nullable, None, None)
    }

  implicit val datetime: DriverColumnDefiner[MysqlDriver.type, DATETIME.type] =
    new DriverColumnDefiner[MysqlDriver.type, DATETIME.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DATETIME", nullable, None, None)
    }

  implicit val text: DriverColumnDefiner[MysqlDriver.type, TEXT.type] =
    new DriverColumnDefiner[MysqlDriver.type, TEXT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "TEXT", nullable, None, None)
    }

  implicit val double: DriverColumnDefiner[MysqlDriver.type, DOUBLE.type] =
    new DriverColumnDefiner[MysqlDriver.type, DOUBLE.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DOUBLE", nullable, None, None)
    }
}

case object MysqlDriver extends JdbcDriver with MysqlTableCreationImplicits {
  override val driver = DriverClass("com.mysql.jdbc.Driver")
  override def getTableDesc(
    tableName: TableName,
    columnNames: Array[ColumnName],
    columnDefinitions: Array[Definition]) =
    new TableDesc(
      tableName.get,
      columnNames.map(_.get),
      columnDefinitions.map(_.get),
      null,
      "SHOW TABLES LIKE '%s'")
}

case object HsqlDbDriver extends JdbcDriver {
  override val driver = DriverClass("org.hsqldb.jdbcDriver")
}

/**
 * Old Vertica 4.1 jdbc driver
 * see https://my.vertica.com/docs/5.1.6/HTML/index.htm#16699.htm
 */
case object VerticaDriver extends JdbcDriver {
  override val driver = DriverClass("com.vertica.Driver")
}

/**
 * Vertica jdbc driver (5.1 and higher)
 */
case object VerticaJdbcDriver extends JdbcDriver {
  override val driver = DriverClass("com.vertica.jdbc.Driver")
}
