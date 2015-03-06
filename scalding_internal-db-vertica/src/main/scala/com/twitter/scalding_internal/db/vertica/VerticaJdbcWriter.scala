package com.twitter.scalding_internal.db.vertica

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

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
  extends JdbcWriter(tableName, connectionConfig, columns, addlQueries) {

  import CloseableHelper._
  import TryHelper._

  private val log = LoggerFactory.getLogger(this.getClass)

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

  override protected def sqlTableCreateStmt: SqlQuery = {
    val allCols = columns.map(_.name).zip(colsToDefs(columns))
      .map { case (ColumnName(name), Definition(defn)) => s"""  ${name}  $defn""" }
      .mkString(",\n|")

    SqlQuery(s"""
      |create TABLE IF NOT EXISTS ${schema.toStr}.${tableName.toStr} (
      |$allCols
      |)
      """.stripMargin('|'))
  }

  override protected def createTableIfNotExists: Try[Unit] = {
    log.info(sqlTableCreateStmt.toStr)
    runQuery(SqlQuery(sqlTableCreateStmt.toStr))
  }

  def load(hadoopUri: HadoopUri, conf: JobConf): Try[Int] = {

    val fs = FileSystem.get(conf)
    val federatedName = fs.resolvePath(new Path(hadoopUri.toStr)).toString.replaceAll("hdfs://", "").split("/")(0)
    val httpPath = conf.get(s"dfs.namenode.http-address.${federatedName}.nn1")
    val httpHdfsUrl = s"""http://${httpPath}/webhdfs/v1${hadoopUri.toStr}/part-*"""

    val runningAsUserName = System.getProperty("user.name")
    for {
      conn <- jdbcConnection
      _ <- runCmd(conn, sqlTableCreateStmt.toStr).onFailure(conn.close())
      loadSqlStatement = s"""COPY ${schema.toStr}.${tableName.toStr} NATIVE with SOURCE Hdfs(url='$httpHdfsUrl', username='$runningAsUserName') ABORT ON ERROR"""
      // abort on error - if any single row has a schema mismatch, vertica rolls back the transaction and fails
      loadedCount <- runCmd(conn, loadSqlStatement).onComplete(conn.close())
    } yield loadedCount
  }
}
