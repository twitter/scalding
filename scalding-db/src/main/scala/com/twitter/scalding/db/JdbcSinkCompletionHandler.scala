package com.twitter.scalding.db

import java.io.IOException;
import scala.util.{ Failure, Success }

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.LoggerFactory

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
