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

package com.twitter.scalding_internal.db.jdbc

import java.sql.{ DriverManager, ResultSet, ResultSetMetaData }
import scala.util.{ Failure, Success, Try }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, FSDataOutputStream, Path }
import org.slf4j.LoggerFactory

import com.twitter.scalding_internal.db._

import JsonUtils._

object JdbcToHdfsCopier {

  protected val log = LoggerFactory.getLogger(this.getClass)

  def apply[T <: AnyRef: Manifest](connectionConfig: ConnectionConfig,
    selectQuery: String, hdfsPath: Path,
    charSet: String, recordsPerFile: Option[Int])(validator: Option[ResultSetMetaData => Try[Unit]], rs2CaseClass: ResultSet => T): Unit = {

    log.info(s"Starting jdbc to hdfs copy - $hdfsPath")
    Try(DriverManager.getConnection(connectionConfig.connectUrl.toStr,
      connectionConfig.userName.toStr,
      connectionConfig.password.toStr)).map { conn =>
      // note: this is specific to mysql's jdbc driver implementation
      // and may not work entirely well with other dbs.
      // Other db specific copiers could be added in the future if needed.
      val stmt = conn.createStatement(
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
      stmt.setFetchSize(Integer.MIN_VALUE)
      // integer min_value is a magic number needed by
      // mysql jdbc driver to do streaming reads
      // instead of pulling entire table at one go.
      // see: http://stackoverflow.com/a/20496877/2336541

      log.info(s"Executing query $selectQuery")
      val rs: ResultSet = stmt.executeQuery(selectQuery)
      validator.foreach { _(rs.getMetaData).get }
      writeToHdfs[T](rs, hdfsPath, recordsPerFile, charSet)(rs2CaseClass)
    } match {
      case Success(s) => ()
      case Failure(e) => throw new java.lang.IllegalArgumentException(s"Failed - ${e.getMessage}", e)
    }
  }

  protected def writeToHdfs[T <: AnyRef: Manifest](rs: ResultSet, hdfsPath: Path, recordsPerFile: Option[Int],
    charSet: String)(rs2CaseClass: ResultSet => T): Unit = {

    lazy val inj = caseClass2Json[T]

    val fsconf = new Configuration
    val fs = FileSystem.get(fsconf)
    def getPartFile(p: Int): FSDataOutputStream =
      fs.create(new Path(f"$hdfsPath/part-$p%05d"))

    var part = 0
    var count = 0
    var hdfsStagingFile = getPartFile(part)
    while (rs.next) {
      val output = rs2CaseClass(rs)
      hdfsStagingFile.write(s"${inj.apply(output)}\n".getBytes(charSet))
      count = count + 1
      if (Some(count) == recordsPerFile) {
        hdfsStagingFile.close()
        count = 0
        part = part + 1
        hdfsStagingFile = getPartFile(part)
      }
    }
    hdfsStagingFile.close()
    val successFile = fs.create(new Path(hdfsPath + "/_SUCCESS"))
    successFile.close()
  }
}

