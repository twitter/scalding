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

import java.sql._
import scala.util.{ Failure, Success, Try }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.slf4j.LoggerFactory

import com.twitter.scalding_internal.db._

object JdbcToHdfsCopier {

  protected val log = LoggerFactory.getLogger(this.getClass)

  // TODO: support partition sizes
  def apply(connectionConfig: ConnectionConfig, selectQuery: String, hdfsPath: Path)(rs2String: ResultSet => String): Unit = {
    log.info(s"Starting jdbc to hdfs copy - $hdfsPath")
    Try(DriverManager.getConnection(connectionConfig.connectUrl.toStr,
      connectionConfig.userName.toStr,
      connectionConfig.password.toStr)).map { conn =>
      val fsconf = new Configuration
      val fs = FileSystem.get(fsconf)
      val hdfsStagingFile = fs.create(new Path(hdfsPath + "/part-00000"))
      val stmt = conn.createStatement
      stmt.setFetchSize(Integer.MIN_VALUE) // don't pull entire table into memory
      log.info(s"Executing query $selectQuery")
      val rs = stmt.executeQuery(selectQuery)
      while (rs.next) {
        val output = rs2String(rs)
        hdfsStagingFile.write(s"$output".getBytes)
      }
      hdfsStagingFile.close
      val successFile = fs.create(new Path(hdfsPath + "/_SUCCESS"))
      successFile.close
    } match {
      case Success(s) => s
      case Failure(e) => throw new java.lang.IllegalArgumentException(s"Failed - ${e.getMessage}", e)
    }
  }
}

