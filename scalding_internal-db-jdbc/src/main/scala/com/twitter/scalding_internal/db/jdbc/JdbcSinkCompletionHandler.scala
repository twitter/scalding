package com.twitter.scalding_internal.db.jdbc

import java.io.IOException;
import scala.util.{ Failure, Success }

import org.apache.hadoop.mapred.JobConf;

case class JdbcSinkCompletionHandler(loader: JdbcLoader) {

  def commitResource(conf: JobConf, path: String): Boolean =
    loader.runLoad(HadoopUri(path)) match {
      case Success(l) =>
        println(s"Wrote $l entries to vertica")
        true
      case Failure(e) =>
        throw e
        false
    }
}
