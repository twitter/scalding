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
package com.twitter.lui.column_reader

import java.lang.String.format
import org.apache.parquet.Log.DEBUG
import org.apache.parquet.Preconditions.checkNotNull
import org.apache.parquet.column.ValuesType.DEFINITION_LEVEL
import org.apache.parquet.column.ValuesType.REPETITION_LEVEL
import org.apache.parquet.column.ValuesType.VALUES

import java.io.ByteArrayInputStream
import java.io.IOException

import org.apache.parquet.CorruptDeltaByteArrays
import org.apache.parquet.Log
import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Dictionary
import org.apache.parquet.column.EncodingAccessor
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

object BaseColumnReader {
  private val definitionLevelCacheSize = 128
  private val LOG: Log = Log.getLog(getClass)
}

abstract class BaseColumnReader(
  writerVersion: ParsedVersion,
  path: ColumnDescriptor,
  pageReader: PageReader,
  val maxDefinitionLevel: Int,
  val maxRepetitionLevel: Int) extends SpecificTypeGet {

  def columnName: String = path.getPath.mkString("/")

  private[this] lazy val totalValueCount = pageReader.getTotalValueCount

  protected lazy val dictionary = Option(pageReader.readDictionaryPage).map { dictionaryPage =>
    val dictionary = dictionaryPage.getEncoding.initDictionary(path, dictionaryPage)
    dictionary
  }.orNull

  import BaseColumnReader._

  private[this] val repetitionLevels = new Array[Int](definitionLevelCacheSize)
  private[this] val definitionLevels = new Array[Int](definitionLevelCacheSize)

  private[this] var levelIdx: Int = _
  private[this] var readValues: Int = _

  private[this] var r_decoder: RunLengthBitPackingHybridDecoder = _
  private[this] var d_decoder: RunLengthBitPackingHybridDecoder = _

  protected var currentEncoding: Encoding = _

  private[this] var endOfPageValueCount: Long = 0L
  private[this] var pageValueCount: Int = 0

  protected var dataColumn: ValuesReader = _

  private[this] var mutRecordPosition = -1L

  protected var dictionaryActive: Boolean = false

  // Tests if the target is behind the state
  protected def isBehind(shouldBeAhead: Array[Int], shouldBeLessOrEqual: Array[Int]): Boolean = {
    val minLen = if (shouldBeLessOrEqual.length < shouldBeAhead.length) shouldBeLessOrEqual.length else shouldBeAhead.length
    var p = 0
    while (p < minLen) {
      if (shouldBeLessOrEqual(p) > shouldBeAhead(p))
        return true
      p += 1
    }
    false
  }

  @inline
  protected final def recordPosition = mutRecordPosition

  def getRecordPosition = mutRecordPosition

  def getPrimitivePosition = readValues

  def advanceSetRecord(newRecordPosition: Long): Unit

  def primitiveAdvance(targetRecord: Long, targetOffset: Array[Int]): Boolean

  protected final def skip(): Unit = {
    if (dataColumn == null) {
      mutRecordPosition = 0L
      loadData()
    } else {
      if (getCurrentDefinitionLevel == maxDefinitionLevel)
        dataColumn.skip()
      advanceDefRef()
    }
  }

  final def isFullyConsumed: Boolean = readValues >= totalValueCount

  protected final def getCurrentRepetitionLevel: Int = repetitionLevels(levelIdx)

  protected final def getCurrentDefinitionLevel: Int = definitionLevels(levelIdx)

  final def isPageFullyConsumed: Boolean = readValues >= endOfPageValueCount

  final def getTotalValueCount: Long = totalValueCount

  final def isNull: Boolean = getCurrentDefinitionLevel != maxDefinitionLevel

  protected def advanceDefRef(): Boolean = {
    levelIdx += 1
    if (levelIdx > 0 && isFullyConsumed) sys.error("Consuming empty iterator")

    readValues += 1

    val r = if (levelIdx >= definitionLevelCacheSize || isPageFullyConsumed) {
      loadData
    } else true

    if (getCurrentRepetitionLevel == 0) {
      mutRecordPosition += 1L
    }
    r
  }

  private[this] def updateDataAvailable(): Long = {
    val pageRemaining: Long = endOfPageValueCount - readValues
    if (pageRemaining > 0)
      pageRemaining
    else {
      // If no data left we read the next page.
      // to do this we will read in a new data column too,
      // so must have advanced the data reader on before hand
      // To call here we have to actually have consumed
      // all the values aswell as the def/rep level info
      // so its safe to just move on here
      checkRead()
      endOfPageValueCount - readValues
    }
  }

  private[this] def loadData(): Boolean = {
    val pageRemaining = updateDataAvailable()

    if (pageRemaining > 0) {
      if (r_decoder != null)
        r_decoder.readInts(repetitionLevels)
      if (d_decoder != null)
        d_decoder.readInts(definitionLevels)
      levelIdx = 0
      true
    } else {
      false
    }
  }

  private[this] def checkRead(): Unit = {
    if (isPageFullyConsumed) {
      if (isFullyConsumed) {
        repetitionLevels(0) = 0
        definitionLevels(0) = 0
        levelIdx = 0
      } else
        readPage()
    }
  }

  private[this] def readPage(): Unit =
    pageReader.readPage() match {
      case pageV1: DataPageV1 => readPageV1(pageV1)
      case p => sys.error(s"unknown page type $p")
    }

  var pageIdx = -1
  private[this] def readPageV1(page: DataPageV1) {
    try {
      levelIdx = 9999 // reset where we are in our cache of info from r_decoder and d_decoder
      var offset: Int = 0
      pageIdx += 1
      println(s"Loading page $pageIdx for column $columnName")
      val bytes = page.getBytes().toByteArray()

      val repetitionLevelEncoding = page.getRlEncoding
      val definitionLevelEncoding = page.getRlEncoding
      require(repetitionLevelEncoding == Encoding.RLE || repetitionLevelEncoding == Encoding.BIT_PACKED, s"repetitionLevelEncoding encoding should be the Hybrid runlength bit packing decoder, was ${repetitionLevelEncoding.toString}")
      require(definitionLevelEncoding == Encoding.RLE || definitionLevelEncoding == Encoding.BIT_PACKED, s"definitionLevelEncoding encoding should be the Hybrid runlength bit packing decoder, was ${repetitionLevelEncoding.toString}")

      val r_bitWidth: Int = BytesUtils.getWidthFromMaxInt(EncodingAccessor.getMaxLevel(repetitionLevelEncoding, path, REPETITION_LEVEL))

      if (r_bitWidth > 0) {
        val r_in: ByteArrayInputStream = new ByteArrayInputStream(bytes, offset, bytes.length - offset)
        val r_length: Int = BytesUtils.readIntLittleEndian(r_in)
        offset += 4; // 4 is for the length which is stored as 4 bytes little endian
        r_decoder = new RunLengthBitPackingHybridDecoder(r_bitWidth, bytes, offset, offset + r_length)
        offset += r_length
      } else {
        r_decoder = null
      }

      val d_bitWidth: Int = BytesUtils.getWidthFromMaxInt(EncodingAccessor.getMaxLevel(repetitionLevelEncoding, path, DEFINITION_LEVEL))

      if (d_bitWidth > 0) {
        val d_in: ByteArrayInputStream = new ByteArrayInputStream(bytes, offset, bytes.length - offset)
        val d_length: Int = BytesUtils.readIntLittleEndian(d_in)
        offset += 4; // 4 is for the length which is stored as 4 bytes little endian
        d_decoder = new RunLengthBitPackingHybridDecoder(d_bitWidth, bytes, offset, offset + d_length)
        offset += d_length
      } else {
        d_decoder = null
      }

      if (DEBUG) LOG.debug("reading data at " + offset)
      initDataReader(page.getValueEncoding(), bytes, offset, page.getValueCount())
    } catch {
      case e: IOException =>
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e)
    }
  }

  private[this] def initDataReader(dataEncoding: Encoding, bytes: Array[Byte], offset: Int, valueCount: Int) {
    this.currentEncoding = dataEncoding
    this.pageValueCount = valueCount
    this.endOfPageValueCount = readValues + pageValueCount

    dataColumn = dataEncoding.usesDictionary match {
      case true if dictionary != null =>
        dictionaryActive = true
        dataEncoding.getDictionaryBasedValuesReader(path, VALUES, dictionary)
      case false =>
        dictionaryActive = false
        dataEncoding.getValuesReader(path, VALUES)
      case _ => sys.error("Tried to use dictionary encoding but have no dictionary")
    }

    dataColumn.initFromPage(pageValueCount, bytes, offset)
  }
}
