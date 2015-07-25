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

import org.apache.hadoop.hdfs.DFSUtil
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

import java.net.{ HttpURLConnection, InetSocketAddress, URL }
import scala.collection.JavaConverters._
import scala.util.Try

object HdfsUtil {

  private val log = LoggerFactory.getLogger(this.getClass)

  // finds the active namenode from addrs provided
  // and returns the webhdfs url for it
  private def findActiveNnUrl(addrs: Iterable[InetSocketAddress], conf: JobConf): Option[String] =
    addrs
      .map { addr =>
        // convert rpc address to http
        val httpUrl = DFSUtil.getInfoServer(addr, conf, "http").toString
        s"${httpUrl}/webhdfs/v1"
      }
      .find { url =>
        // liststatus used as ping operation
        val checkUrl = s"$url/?OP=LISTSTATUS"
        log.debug(s"Checking namenode status: $checkUrl")
        try {
          val conn = new URL(checkUrl).openConnection.asInstanceOf[HttpURLConnection]
          conn.setRequestMethod("GET")
          conn.connect()
          val resp = conn.getResponseCode
          val msg = conn.getResponseMessage
          conn.disconnect()
          log.debug(s"Received response code $resp: $msg")
          resp == 200
        } catch {
          case _: java.io.IOException =>
            log.info(s"Failed to connect to $checkUrl")
            false
        }
      }

  // returns wdbhdfs url for the provided path name
  def webhdfsUrl(federatedName: String, conf: JobConf): Try[String] = Try {
    val nnRpcAddrs = DFSUtil.getHaNnRpcAddresses(conf).asScala
    val activeNn = nnRpcAddrs.get(federatedName) match {
      // returns namenode -> address map for each nn configured
      case Some(nnmap) => findActiveNnUrl(nnmap.asScala.values, conf)
      case None => sys.error(s"No namenodes found for federated name $federatedName in current configuration $nnRpcAddrs")
    }
    activeNn match {
      case Some(nn) =>
        log.info(s"Found active namenode $nn")
        nn
      case None =>
        sys.error(s"No active namenode found for federated name: $federatedName")
    }
  }
}
