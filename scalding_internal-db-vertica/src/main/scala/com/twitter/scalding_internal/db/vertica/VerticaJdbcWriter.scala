package com.twitter.scalding_internal.db.vertica

import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.extensions.VerticaExtensions
import com.twitter.scalding_internal.db.jdbc._
import com.twitter.scalding_internal.db.jdbc.driver.DriverClass

import java.sql._
import java.util.Properties
import scala.util.{ Try, Success, Failure }

class VerticaJdbcWriter(tableName: TableName,
  schema: SchemaName,
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition],
  addlQueries: AdditionalQueries)
  extends JdbcWriter(tableName, Some(schema), connectionConfig, columns, addlQueries) {

  import CloseableHelper._
  import TryHelper._

  protected[this] val driverClassName = DriverClass("com.vertica.jdbc.Driver")

  override def colsToDefs(columns: Iterable[ColumnDefinition]) =
    DBColumnTransformer.columnDefnsToCreate(VerticaExtensions.verticaMutator, columns)

  private def runCmd(conn: Connection, sql: String): Try[Int] = {
    val statement = conn.createStatement
    println("Executing sql: \n" + sql + "\n")
    Try(statement.execute(sql))
      .map { _ => Try(statement.getUpdateCount).getOrElse(-1) }
      .onComplete(statement.close())
  }

  def load(hadoopUri: HadoopUri): Try[Int] = {
    val runningAsUserName = System.getProperty("user.name")
    for {
      conn <- jdbcConnection
      _ <- runCmd(conn, sqlTableCreateStmt).onFailure(conn.close())
      loadSqlStatement = s"""COPY ${schema.toStr}.${tableName.toStr} SOURCE Hdfs(url='$hadoopUri', username='$runningAsUserName') DELIMITER E'\t' ABORT ON ERROR"""
      // abort on error - if any single row has a schema mismatch, vertica rolls back the transaction and fails
      loadedCount <- runCmd(conn, loadSqlStatement).onComplete(conn.close())
    } yield loadedCount
  }
}
