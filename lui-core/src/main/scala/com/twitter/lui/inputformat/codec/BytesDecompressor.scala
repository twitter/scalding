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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionOutputStream
import org.apache.hadoop.io.compress.Compressor
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.util.ReflectionUtils

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class BytesDecompressor(codec: CompressionCodec) {
  private[this] val decompressor: Decompressor = if (codec != null) {
    CodecPool.getDecompressor(codec)
  } else null

  def decompress(bytes: BytesInput, uncompressedSize: Int): BytesInput =
    if (codec != null) {
      decompressor.reset()
      val is: InputStream = codec.createInputStream(new ByteArrayInputStream(bytes.toByteArray()), decompressor)
      BytesInput.from(is, uncompressedSize)
    } else {
      bytes
    }

  private[codec] def release() {
    if (decompressor != null) {
      CodecPool.returnDecompressor(decompressor)
    }
  }
}
