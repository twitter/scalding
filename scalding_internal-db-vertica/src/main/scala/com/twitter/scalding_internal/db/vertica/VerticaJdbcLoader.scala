package com.twitter.scalding_internal.db.vertica

import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.extensions.VerticaExtensions
import com.twitter.scalding_internal.db.jdbc._

import java.sql._
import java.util.Properties
import scala.util.{ Try, Success, Failure }

class VerticaJdbcLoader(tableName: TableName,
  schema: SchemaName,
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition],
  preloadQuery: Option[SqlQuery],
  postloadQuery: Option[SqlQuery])
  extends JdbcLoader(tableName, Some(schema), connectionConfig, columns, preloadQuery, postloadQuery) {

  val driverClassName = "com.vertica.jdbc.Driver"

  override def colsToDefs(columns: Iterable[ColumnDefinition]) =
    DBColumnTransformer.columnDefnsToCreate(VerticaExtensions.verticaMutator, columns)

  private def runCmd(conn: Connection, sql: String): Try[Int] = {
    val statement = conn.createStatement
    println("Executing sql: \n" + sql + "\n")
    val ret = Try(statement.execute(sql)).map { _ =>
      Try(statement.getUpdateCount).getOrElse(-1)
    }
    Try(statement.close)
    ret
  }

  def load(hadoopUri: HadoopUri): Try[Int] = {
    val runningAsUserName = System.getProperty("user.name")
    val connTry = jdbcConnection
    val tryInt = for {
      conn <- connTry
      _ <- runCmd(conn, sqlTableCreateStmt)
      loadSqlStatement = s"""COPY ${schema.toStr}.${tableName.toStr} SOURCE Hdfs(url='${hadoopUri.toStr}', username='$runningAsUserName') DELIMITER E'\t'"""
      loadedCount <- runCmd(conn, loadSqlStatement)
    } yield loadedCount

    connTry.foreach(t => Try(t.close))
    tryInt
  }
}
