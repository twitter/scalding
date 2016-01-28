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
import com.twitter.lui.column_reader._

object CacheColumnReader {
  private val LOG: Log = Log.getLog(getClass)
}

abstract class CacheColumnReader(
  writerVersion: ParsedVersion,
  path: ColumnDescriptor,
  pageReader: PageReader,
  maxDefinitionLevel: Int,
  maxRepetitionLevel: Int) extends BaseColumnReader(writerVersion, path, pageReader, maxDefinitionLevel, maxRepetitionLevel) with SpecificTypeGet with HasCache {

  import CacheColumnReader._

  protected var cacheRecordPosition = -1L

  protected var cacheSize: Int = 0

  protected var cacheDef: Array[Int] = new Array[Int](8)
  protected var cacheRep: Array[Int] = new Array[Int](8)

  private[this] def grow(cachePos: Int): Unit = {
    if (cachePos >= cacheDef.length) {
      val oldCacheDef = cacheDef
      val oldCacheRep = cacheRep
      val newSize = cacheDef.length * 2

      cacheDef = new Array[Int](newSize)
      cacheRep = new Array[Int](newSize)

      System.arraycopy(oldCacheDef, 0, cacheDef, 0, oldCacheDef.length)
      System.arraycopy(oldCacheRep, 0, cacheRep, 0, oldCacheDef.length)
    }
  }

  def populateCache(): Unit = {

    cacheRecordPosition = recordPosition
    var cacheIdx = 0
    while (recordPosition == cacheRecordPosition && !isFullyConsumed) {
      grow(cacheIdx)
      cacheDef(cacheIdx) = getCurrentDefinitionLevel
      cacheRep(cacheIdx) = getCurrentRepetitionLevel
      if (getCurrentDefinitionLevel == maxDefinitionLevel) {
        storeVToCache(cacheIdx)
      }
      advanceDefRef()
      cacheIdx += 1
    }
    cacheSize = cacheIdx
  }

  override def getRecordPosition = cacheRecordPosition

  def advanceSetRecord(newRecordPosition: Long): Unit = {
    if (newRecordPosition != cacheRecordPosition) {
      if (newRecordPosition < cacheRecordPosition) {
        sys.error(s"$columnName -> Attempted to access an old field, requested record $newRecordPosition, while the cache contains $cacheRecordPosition")
      }

      while (recordPosition < newRecordPosition) {
        skip()
      }

      if (!isFullyConsumed) {
        populateCache()
        cachePrimitivePosition(0) = 0
        var p = 0
        while (p < (maxRepetitionLevel + 1)) {
          cachePrimitiveState(p) = 0
          p += 1
        }
      }
    }
  }

  def notNull(definitionLevel: Int, recordIdx: Long): Boolean = {
    advanceSetRecord(recordIdx)
    cacheDef(0) >= definitionLevel
  }

  def notNull(definitionLevel: Int,
    recordIdx: Long,
    cachePos: Array[Int],
    stateOffsets: Array[Int],
    targetOffsets: Array[Int]): Boolean = {
    advanceSetRecord(recordIdx)
    skipInsideRecord(definitionLevel, cachePos, stateOffsets, targetOffsets)
  }

  protected var cachePrimitivePosition = Array(0)
  var cachePrimitiveState: Array[Int] = (0 until (maxRepetitionLevel + 1)).map(_ => 0).toArray

  def primitiveAdvance(targetRecord: Long, targetOffset: Array[Int]): Boolean = {
    advanceSetRecord(targetRecord)
    if (!isBehind(targetOffset, cachePrimitiveState)) {
      cachePrimitivePosition(0) = 0
      var p = 0
      while (p < cachePrimitiveState.length) {
        cachePrimitiveState(p) = 0
        p += 1
      }
    }
    skipInsideRecord(maxDefinitionLevel, cachePrimitivePosition, cachePrimitiveState, targetOffset)
  }

  def skipInsideRecord(definitionLevel: Int, previousPosition: Array[Int], stateOffset: Array[Int], targetOffset: Array[Int]): Boolean = {

    var p = 0
    val maxLen = (maxRepetitionLevel + 1)
    var cachePosition: Int = previousPosition(0)
    val cacheSize = this.cacheSize

    while (p < maxLen) {
      if (stateOffset(p) > targetOffset(p)) {
        previousPosition(0) = cachePosition
        return false
      }

      // the first one at any level won't have the flag set
      while (stateOffset(p) < targetOffset(p)) {
        // If we got onto a new record
        // or the repetition level is such that we are looping further out than
        // this advance calls for.
        // The 0th one will occur on the first time, and is a red herring.
        // we account for that by having the cache position be < the cache size
        if (cachePosition >= cacheSize) {
          previousPosition(0) = cachePosition
          return false
        } else {
          cachePosition += 1
          if (cachePosition >= cacheSize) {
            previousPosition(0) = cachePosition
            return false
          }

          val cRep = cacheRep(cachePosition)

          stateOffset(cRep) += 1
          var p2 = cRep + 1
          while (p2 < stateOffset.length) {
            stateOffset(p2) = 0
            p2 += 1
          }
          // if we are on an outer loop then we are done, nothing to find
          if (cRep < p) {
            previousPosition(0) = cachePosition
            return false
          }
        }
      }
      p += 1;
    }
    // Now we have gotten to the best place we can, is it null?
    previousPosition(0) = cachePosition
    cacheDef(cachePosition) >= definitionLevel
  }

}
