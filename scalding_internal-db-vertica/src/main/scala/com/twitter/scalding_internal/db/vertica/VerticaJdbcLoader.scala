package com.twitter.scalding_internal.db.vertica
import java.sql._
import java.util.Properties
import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.extensions.VerticaExtensions
import scala.util.{ Try, Success, Failure }

class VerticaJdbcLoader(tableName: TableName,
  schema: VerticaSchema,
  connectionConfig: ConnectionConfig,
  columns: Iterable[ColumnDefinition]) {

  private def colsToDefs(columns: Iterable[ColumnDefinition]) =
    DBColumnTransformer.columnDefnsToCreate(VerticaExtensions.verticaMutator, columns)

  private val columnDefinitions = colsToDefs(columns)

  private val sqlTableCreateStmt = {
    val allCols = columns.map(_.name).zip(colsToDefs(columns))
      .map { case (ColumnName(name), Definition(defn)) => s"""  "${name}"  $defn""" }
      .mkString(",\n|")

    s"""
      |create TABLE IF NOT EXISTS ${schema.toStr}.${tableName.toStr} (
      |$allCols
      |)
      """.stripMargin('|')
  }

  private val verticaDriverClass: Class[_] = try {
    Class.forName("com.vertica.jdbc.Driver");
  } catch {
    case e: ClassNotFoundException =>
      System.err.println("Could not find the JDBC driver class.");
      e.printStackTrace();
      throw e
  }

  private def runCmd(conn: Connection, sql: String): Try[Int] = {
    val statement = conn.createStatement
    println("Executing sql: \n" + sql + "\n")
    val ret = Try(statement.execute(sql)).map { _ =>
      Try(statement.getUpdateCount).getOrElse(-1)
    }
    Try(statement.close)
    ret
  }

  def load(hadoopUri: String): Try[Int] = {
    val runningAsUserName = System.getProperty("user.name")
    val connTry = Try(DriverManager.getConnection(connectionConfig.connectUrl.toStr,
      connectionConfig.userName.toStr,
      connectionConfig.password.toStr)).map { c =>
      c.setAutoCommit(true)
      c
    }

    val tryInt = for {
      conn <- connTry
      _ <- runCmd(conn, sqlTableCreateStmt)
      loadSqlStatement = s"""COPY ${schema.toStr}.${tableName.toStr} SOURCE Hdfs(url='$hadoopUri', username='$runningAsUserName') DELIMITER E'\t'"""
      loadedCount <- runCmd(conn, loadSqlStatement)
    } yield loadedCount

    connTry.foreach(t => Try(t.close))
    tryInt
  }
}