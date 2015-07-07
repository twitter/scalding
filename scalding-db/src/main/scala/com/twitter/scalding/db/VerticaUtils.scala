package com.twitter.scalding.db

import com.twitter.bijection._
import com.twitter.scalding._
import java.io.OutputStream
import org.apache.hadoop.fs.{ FileSystem, FSDataOutputStream, Path }
import org.apache.hadoop.mapred.JobConf
import scala.util.{ Try, Success, Failure }

trait VerticaHeader[T] extends java.io.Serializable {
  def bytes: Array[Byte]
}

object VerticaUtils {
  val encoder = implicitly[Bijection[Array[Byte], GZippedBase64String]]

  val VERTICA_HEADER_CONTENTS = "com.twitter.scalding.db.HEADER"
  import LittleEndianJavaStreamEnrichments._
  def getOutputStream(conf: JobConf, os: OutputStream): OutputStream = {
    val encodedData = conf.get(VERTICA_HEADER_CONTENTS)
    require(encodedData != null, "Unable to get the header, should be in the jobconf")
    val headerBytes = encoder.invert(GZippedBase64String(encodedData))

    os.writeBytes(headerBytes)
    os
  }

  def setHeaderContents[T](conf: JobConf, header: VerticaHeader[T]): Unit =
    conf.set(VERTICA_HEADER_CONTENTS, encoder(header.bytes).str)

}
