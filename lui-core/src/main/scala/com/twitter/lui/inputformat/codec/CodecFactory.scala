/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.twitter.lui.inputformat.codec

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import scala.collection.mutable.{ Map => MMap }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionOutputStream
import org.apache.hadoop.io.compress.Compressor
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.util.ReflectionUtils

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class CodecFactory(configuration: Configuration) {
  private[this] val decompressors = MMap[CompressionCodecName, BytesDecompressor]()
  private[this] val codecByName = MMap[String, CompressionCodec]()

  /**
   *
   * @param codecName the requested codec
   * @return the corresponding hadoop codec. null if UNCOMPRESSED
   */
  private[this] def getCodec(codecName: CompressionCodecName): CompressionCodec = {
    val codecClassName: String = codecName.getHadoopCompressionCodecClassName
    if (codecClassName == null)
      null
    else
      codecByName.getOrElseUpdate(codecClassName, {
        try {
          ReflectionUtils.newInstance(Class.forName(codecClassName), configuration).asInstanceOf[CompressionCodec]
        } catch {
          case e: ClassNotFoundException => throw new Exception("Class " + codecClassName + " was not found", e)
        }
      })
  }

  def getDecompressor(codecName: CompressionCodecName): BytesDecompressor =
    decompressors.getOrElseUpdate(codecName, {
      new BytesDecompressor(getCodec(codecName))
    })

  def release() {
    decompressors.values.foreach{ decompressor =>
      decompressor.release()
    }
    decompressors.clear()
  }
}
