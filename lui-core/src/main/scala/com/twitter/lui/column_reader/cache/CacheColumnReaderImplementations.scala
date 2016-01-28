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
package com.twitter.lui.column_reader.cache

import com.twitter.lui.column_reader._
import java.lang.String.format
import org.apache.parquet.Log.DEBUG
import org.apache.parquet.Preconditions.checkNotNull
import org.apache.parquet.column.ValuesType.DEFINITION_LEVEL
import org.apache.parquet.column.ValuesType.REPETITION_LEVEL
import org.apache.parquet.column.ValuesType.VALUES
import java.nio.ByteBuffer

import java.io.ByteArrayInputStream
import java.io.IOException

import org.apache.parquet.CorruptDeltaByteArrays
import org.apache.parquet.Log
import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Dictionary
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.DataPageV2
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.page.PageReader
import org.apache.parquet.column.values.RequiresPreviousReader
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeNameConverter

import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.Dictionary
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.Dictionary

final case class BooleanColumnReader(
  writerVersion: ParsedVersion,
  path: ColumnDescriptor,
  pageReader: PageReader,
  maxDefLvl: Int,
  maxRepLvl: Int) extends CacheColumnReader(writerVersion, path, pageReader, maxDefLvl, maxRepLvl) {
  private[this] lazy val dict: Array[Boolean] = if (dictionary == null) null else {
    val maxId = dictionary.getMaxId
    val dict = new Array[Boolean](maxId + 1)
    var i = 0
    while (i <= maxId) {
      dict(i) = dictionary.decodeToBoolean(i)
      i = i + 1
    }
    dict
  }

  override def getBoolean(): Boolean = cacheV(cachePrimitivePosition(0))

  private[this] var cacheV: Array[Boolean] = new Array[Boolean](8)

  private[this] def innerRead(): Boolean =
    if (!dictionaryActive)
      dataColumn.readBoolean
    else {
      dict(dataColumn.readValueDictionaryId)
    }

  override def storeVToCache(position: Int): Unit = {
    if (position >= cacheV.length) {
      val oldCache = cacheV
      cacheV = new Array[Boolean](scala.math.max(position, cacheV.length) * 2)
      System.arraycopy(oldCache, 0, cacheV, 0, oldCache.length)
    }
    cacheV(position) = innerRead()
  }
}

final case class IntColumnReader(
  writerVersion: ParsedVersion,
  path: ColumnDescriptor,
  pageReader: PageReader,
  maxDefLvl: Int,
  maxRepLvl: Int) extends CacheColumnReader(writerVersion, path, pageReader, maxDefLvl, maxRepLvl) {
  private[this] lazy val dict: Array[Int] = if (dictionary == null) null else {
    val maxId = dictionary.getMaxId
    val dict = new Array[Int](maxId + 1)
    var i = 0
    while (i <= maxId) {
      dict(i) = dictionary.decodeToInt(i)
      i = i + 1
    }
    dict
  }

  override def getInteger(): Int = cacheV(cachePrimitivePosition(0))

  private[this] var cacheV: Array[Int] = new Array[Int](8)

  private[this] def innerRead(): Int = dataColumn.readInteger()

  override def storeVToCache(position: Int): Unit = {
    if (position >= cacheV.length) {
      val oldCache = cacheV
      cacheV = new Array[Int](scala.math.max(position, cacheV.length) * 2)
      System.arraycopy(oldCache, 0, cacheV, 0, oldCache.length)
    }
    cacheV(position) = innerRead()
  }
}

final case class LongColumnReader(
  writerVersion: ParsedVersion,
  path: ColumnDescriptor,
  pageReader: PageReader,
  maxDefLvl: Int,
  maxRepLvl: Int) extends CacheColumnReader(writerVersion, path, pageReader, maxDefLvl, maxRepLvl) {
  private[this] lazy val dict: Array[Long] = if (dictionary == null) null else {
    val maxId = dictionary.getMaxId
    val dict = new Array[Long](maxId + 1)
    var i = 0
    while (i <= maxId) {
      dict(i) = dictionary.decodeToLong(i)
      i = i + 1
    }
    dict
  }

  override def getLong(): Long = cacheV(cachePrimitivePosition(0))

  private[this] var cacheV: Array[Long] = new Array[Long](8)

  private[this] def innerRead(): Long = dataColumn.readLong()

  override def storeVToCache(position: Int): Unit = {
    if (position >= cacheV.length) {
      val oldCache = cacheV
      cacheV = new Array[Long](scala.math.max(position, cacheV.length) * 2)
      System.arraycopy(oldCache, 0, cacheV, 0, oldCache.length)
    }
    cacheV(position) = innerRead()
  }
}

final case class DoubleColumnReader(
  writerVersion: ParsedVersion,
  path: ColumnDescriptor,
  pageReader: PageReader,
  maxDefLvl: Int,
  maxRepLvl: Int) extends CacheColumnReader(writerVersion, path, pageReader, maxDefLvl, maxRepLvl) {
  private[this] lazy val dict: Array[Double] = if (dictionary == null) null else {
    val maxId = dictionary.getMaxId
    val dict = new Array[Double](maxId + 1)
    var i = 0
    while (i <= maxId) {
      dict(i) = dictionary.decodeToDouble(i)
      i = i + 1
    }
    dict
  }

  override def getDouble(): Double = cacheV(cachePrimitivePosition(0))

  private[this] var cacheV: Array[Double] = new Array[Double](8)

  private[this] def innerRead(): Double = dataColumn.readDouble()

  override def storeVToCache(position: Int): Unit = {
    if (position >= cacheV.length) {
      val oldCache = cacheV
      cacheV = new Array[Double](scala.math.max(position, cacheV.length) * 2)
      System.arraycopy(oldCache, 0, cacheV, 0, oldCache.length)
    }
    cacheV(position) = innerRead()
  }
}

case class StringBinaryContainer(b: Binary) {
  lazy val toStr = b.toStringUsingUTF8
  lazy val toBB = b.toByteBuffer
}

final case class StringColumnReader(
  writerVersion: ParsedVersion,
  path: ColumnDescriptor,
  pageReader: PageReader,
  maxDefLvl: Int,
  maxRepLvl: Int) extends CacheColumnReader(writerVersion, path, pageReader, maxDefLvl, maxRepLvl) {

  private[this] lazy val dict: Array[StringBinaryContainer] = if (dictionary == null) null else {
    val maxId = dictionary.getMaxId
    val dict = new Array[StringBinaryContainer](maxId + 1)
    var i = 0
    while (i <= maxId) {
      dict(i) = StringBinaryContainer(dictionary.decodeToBinary(i))
      i = i + 1
    }
    dict
  }

  override def getString(): String = cacheV(cachePrimitivePosition(0)).toStr
  override def getBinary(): ByteBuffer = cacheV(cachePrimitivePosition(0)).toBB

  private[this] var cacheV: Array[StringBinaryContainer] = new Array[StringBinaryContainer](8)

  private[this] def innerRead(): StringBinaryContainer = try {
    if (!dictionaryActive)
      StringBinaryContainer(dataColumn.readBytes)
    else {
      dict(dataColumn.readValueDictionaryId)
    }
  } catch {
    case e: Throwable =>
      println(s"""
          Attempted to read value about column: $columnName
          Dictionary is:\n ${Option(dict).map(_.take(10).mkString("\n"))}
          data column is : $dataColumn
          data column uses dictionary: ${currentEncoding.usesDictionary}
          """)
      throw e
  }

  override def storeVToCache(position: Int): Unit = {
    if (position >= cacheV.length) {
      val oldCache = cacheV
      cacheV = new Array[StringBinaryContainer](scala.math.max(position, cacheV.length) * 2)
      System.arraycopy(oldCache, 0, cacheV, 0, oldCache.length)
    }
    cacheV(position) = innerRead()
  }
}
