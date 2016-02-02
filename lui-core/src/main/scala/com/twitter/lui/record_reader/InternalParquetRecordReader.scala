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
package com.twitter.lui.record_reader

import java.io.IOException
import java.util.Collections
import java.util.HashMap
import java.util.HashSet
import java.util.{ List => JList, Map => JMap, Set => JSet }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.parquet.hadoop.UnmaterializableRecordCounter
import org.apache.parquet.Log
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.api.InitContext
import com.twitter.lui.hadoop.{ ReadSupport, ReadContext }
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.io.api.RecordMaterializer.RecordMaterializationException
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.Type
import scala.collection.JavaConverters._

import org.apache.parquet.Log.DEBUG
import com.twitter.lui.inputformat.ParquetInputFormat.STRICT_TYPE_CHECKING
import com.twitter.lui.inputformat.ParquetFileReader
import com.twitter.lui.{ RecordReader, MessageColumnIO, MessageColumnIOFactory }

object InternalParquetRecordReader {
  private val LOG: Log = Log.getLog(getClass)

  def apply[T](readSupport: ReadSupport[T],
    fileSchema: MessageType,
    parquetFileMetadata: FileMetaData,
    file: Path,
    blocks: IndexedSeq[BlockMetaData],
    configuration: Configuration): InternalParquetRecordReader[T] = {

    // initialize a ReadContext for this file
    val fileMetadata: JMap[String, String] = parquetFileMetadata.getKeyValueMetaData

    // Read all columns from disk
    val columnsToReadFromDisk: List[ColumnDescriptor] = fileSchema.getColumns.asScala.toList

    val reader = new ParquetFileReader(configuration, parquetFileMetadata, file, blocks, columnsToReadFromDisk)

    val total: Long = blocks.map(_.getRowCount).sum

    val unmaterializableRecordCounter = new UnmaterializableRecordCounter(configuration, total)
    LOG.info("RecordReader initialized will read a total of " + total + " records.")

    InternalParquetRecordReader[T](
      readSupport,
      file,
      parquetFileMetadata.getCreatedBy,
      fileSchema,
      configuration.getBoolean(STRICT_TYPE_CHECKING, true),
      reader,
      unmaterializableRecordCounter,
      total,
      configuration,
      fileMetadata)
  }

  def toSetMultiMap[K, V](map: JMap[K, V]): JMap[K, JSet[V]] = {
    val setMultiMap = new HashMap[K, JSet[V]]()
    map.entrySet.asScala.foreach { entry =>
      val set = new HashSet[V]()
      set.add(entry.getValue())
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set))
    }
    Collections.unmodifiableMap(setMultiMap)
  }

}

case class InternalParquetRecordReader[T] private (readSupport: ReadSupport[T],
  file: Path,
  createdBy: String,
  fileSchema: MessageType,
  strictTypeChecking: Boolean,
  reader: ParquetFileReader,
  unmaterializableRecordCounter: UnmaterializableRecordCounter,
  total: Long,
  configuration: Configuration,
  fileMetadata: JMap[String, String]) {
  import InternalParquetRecordReader._

  val columnCount: Int = fileSchema.getPaths.size

  val readContext: ReadContext = readSupport.init(new InitContext(
    configuration,
    toSetMultiMap(fileMetadata),
    fileSchema))

  private[this] var currentValue: T = _
  private[this] var current: Long = 0
  private[this] var currentBlock: Int = -1
  private[this] var recordReader: RecordReader[T] = _

  private[this] var totalTimeSpentReadingBytes: Long = 0L
  private[this] var totalTimeSpentProcessingRecords: Long = 0L
  private[this] var startedAssemblingCurrentBlockAt: Long = 0L

  private[this] var totalCountLoadedSoFar: Long = 0L

  private[this] def checkRead() {
    if (current == totalCountLoadedSoFar) {
      if (current != 0) {
        totalTimeSpentProcessingRecords += (System.currentTimeMillis() - startedAssemblingCurrentBlockAt)
        if (Log.INFO) {
          LOG.info(s"Assembled and processed $totalCountLoadedSoFar records from $columnCount columns in $totalTimeSpentProcessingRecords  ms: ${totalCountLoadedSoFar.toFloat / totalTimeSpentProcessingRecords}  rec/ms, ${(totalCountLoadedSoFar.toFloat * columnCount / totalTimeSpentProcessingRecords)} cell/ms")
          val totalTime: Long = totalTimeSpentProcessingRecords + totalTimeSpentReadingBytes
          if (totalTime != 0) {
            val percentReading: Long = 100 * totalTimeSpentReadingBytes / totalTime
            val percentProcessing: Long = 100 * totalTimeSpentProcessingRecords / totalTime
            LOG.info(s"time spent so far $percentReading % reading ($totalTimeSpentReadingBytes ms) and $percentProcessing % processing ($totalTimeSpentProcessingRecords ms)")
          }
        }
      }

      LOG.info("at row " + current + ". reading next block")
      val t0: Long = System.currentTimeMillis()
      val pages: PageReadStore = reader.readNextRowGroup()
      if (pages == null) {
        throw new IOException(s"expecting more rows but reached last block. Read $current out of $total")
      }
      val timeSpentReading: Long = System.currentTimeMillis() - t0
      totalTimeSpentReadingBytes += timeSpentReading
      BenchmarkCounter.incrementTime(timeSpentReading)
      if (Log.INFO) LOG.info("block read in memory in " + timeSpentReading + " ms. row count = " + pages.getRowCount())

      recordReader = MessageColumnIOFactory.getRecordReader[T](
        pages,
        configuration,
        createdBy,
        fileMetadata.asScala.toMap,
        fileSchema,
        readContext,
        readSupport,
        strictTypeChecking)

      startedAssemblingCurrentBlockAt = System.currentTimeMillis()
      totalCountLoadedSoFar += pages.getRowCount()
      currentBlock += 1
    }
  }

  def close() {
    if (reader != null) {
      reader.close()
    }
  }

  def getCurrentValue(): T = currentValue

  def getProgress: Float = (current.toFloat / total)

  private[this] def contains(group: GroupType, path: Array[String], index: Int): Boolean =
    if (index == path.length) {
      false
    } else {
      if (group.containsField(path(index))) {
        val tpe: Type = group.getType(path(index))
        if (tpe.isPrimitive) {
          return index + 1 == path.length
        } else {
          return contains(tpe.asGroupType, path, index + 1)
        }
      }
      false
    }

  def nextKeyValue(): Boolean = {
    var recordFound: Boolean = false

    // if there are records left and we haven't found one yet
    while (current < total && !recordFound) {

      try {
        checkRead()
        current += 1

        currentValue = recordReader.read() // might fail with a RecordMaterializationException

        if (currentValue == null) {
          // only happens with FilteredRecordReader at end of block
          current = totalCountLoadedSoFar
          if (DEBUG) LOG.debug("filtered record reader reached end of block")
        } else {

          recordFound = true
        }

        if (DEBUG) LOG.debug("read value: " + currentValue)
      } catch {
        case e: RecordMaterializationException =>
          // this might throw, but it's fatal if it does.
          unmaterializableRecordCounter.incErrors(e)
          if (DEBUG) LOG.debug("skipping a corrupt record")

        case e: RuntimeException =>
          throw new ParquetDecodingException(s"Can not read value at $current in block $currentBlock in file $file", e)
      }
    }
    recordFound
  }

}
