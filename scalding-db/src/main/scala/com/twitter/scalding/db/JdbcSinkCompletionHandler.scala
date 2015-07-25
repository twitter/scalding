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

import java.io.IOException;
import scala.util.{ Failure, Success }

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.LoggerFactory

/**
 * Completion handler used with JDBC sinks for executing
 * additional operations after the associated MR step completes.
 *
 * The associated sink is marked as committed after the additional
 * operations have been successfully executed.
 */
case class JdbcSinkCompletionHandler(writer: JdbcWriter) {

  private val log = LoggerFactory.getLogger(this.getClass)

  def commitResource(conf: JobConf, path: String): Boolean =
    writer.run(HadoopUri(path), conf) match {
      case Success(l) =>
        log.info(s"Wrote $l entries to jdbc database")
        true
      case Failure(e) =>
        log.error(s"Error while committing JdbcSinkCompletion: ${e.getMessage}", e)
        false
    }
}
