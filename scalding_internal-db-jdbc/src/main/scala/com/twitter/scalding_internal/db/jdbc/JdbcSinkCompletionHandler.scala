package com.twitter.scalding_internal.db.jdbc

import java.io.IOException;
import scala.util.{ Failure, Success }

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.LoggerFactory

import com.twitter.scalding_internal.db.HadoopUri

case class JdbcSinkCompletionHandler(loader: JdbcLoader) {

  private val log = LoggerFactory.getLogger(this.getClass)

  def commitResource(conf: JobConf, path: String): Boolean =
    loader.runLoad(HadoopUri(path)) match {
      case Success(l) =>
        log.info(s"Wrote $l entries to jdbc database")
        true
      case Failure(e) =>
        log.error(s"Error while committing JdbcSinkCompletion: ${e.getMessage}")
        false
    }
}
