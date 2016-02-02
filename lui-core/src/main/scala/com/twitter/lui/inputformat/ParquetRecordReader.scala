/**
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
package com.twitter.lui.inputformat

import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.format.converter.ParquetMetadataConverter.range
import org.apache.parquet.hadoop.ParquetFileReader.readFooter
import com.twitter.lui.inputformat.ParquetInputFormat.SPLIT_FILES

import java.io.IOException
import java.util.ArrayList
import java.util.Arrays
import java.util.{ List => JList, Map => JMap, Set => JSet, HashSet }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskInputOutputContext

import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.parquet.CorruptDeltaByteArrays
import org.apache.parquet.Log
import org.apache.parquet.column.Encoding
import org.apache.parquet.filter.UnboundRecordFilter
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import com.twitter.lui.hadoop.ReadSupport
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.MessageType
import com.twitter.lui.record_reader.InternalParquetRecordReader
import org.apache.parquet.hadoop.{ ParquetInputSplit, ParquetInputSplitBridge }
import scala.collection.JavaConverters._

/**
 * Reads the records from a block of a Parquet file
 *
 * @see ParquetInputFormat
 *
 * @author Julien Le Dem
 *
 * @param [T] type of the materialized records
 */
object ParquetRecordReader {
  private val LOG: Log = Log.getLog(getClass)

}
case class ParquetRecordReader[T](readSupport: ReadSupport[T]) extends RecordReader[Void, T] {

  var internalReader: InternalParquetRecordReader[T] = _
  import ParquetRecordReader._

  override def close() {
    internalReader.close()
  }

  /**
   * always returns null
   */
  override def getCurrentKey(): Void =
    null

  override def getCurrentValue(): T = internalReader.getCurrentValue()

  override def getProgress(): Float = internalReader.getProgress

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    context match {
      case taskContext: TaskInputOutputContext[_, _, _, _] => BenchmarkCounter.initCounterFromContext(taskContext)
      case _ =>
        LOG.error("Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is "
          + context.getClass().getCanonicalName())
    }

    initializeInternalReader(toParquetSplit(inputSplit), ContextUtil.getConfiguration(context))
  }

  def initialize(inputSplit: InputSplit, configuration: Configuration, reporter: Reporter) {
    BenchmarkCounter.initCounterFromReporter(reporter, configuration)
    initializeInternalReader(toParquetSplit(inputSplit), configuration)
  }

  private[this] def initializeInternalReader(split: ParquetInputSplit, configuration: Configuration) {
    val path: Path = split.getPath()
    val rowGroupOffsets: Array[Long] = split.getRowGroupOffsets
    // if task.side.metadata is set, rowGroupOffsets is null
    val (blocks: IndexedSeq[BlockMetaData], footer: ParquetMetadata) = if (rowGroupOffsets == null) {
      // then we need to apply the predicate push down filter
      val footer = readFooter(configuration, path, range(split.getStart, split.getEnd))
      (footer.getBlocks.asScala.toIndexedSeq, footer)
    } else {
      // otherwise we find the row groups that were selected on the client
      val footer = readFooter(configuration, path, NO_FILTER)
      val offsets: JSet[Long] = new HashSet[Long]()
      rowGroupOffsets.foreach{ offset =>
        offsets.add(offset)
      }
      val blocks =
        footer.getBlocks().asScala.flatMap { block: BlockMetaData =>
          if (offsets.contains(block.getStartingPos())) {
            Some(block)
          } else None
        }.toIndexedSeq

      // verify we found them all
      if (blocks.size != rowGroupOffsets.length) {
        val foundRowGroupOffsets = new Array[Long](footer.getBlocks().size)

        (0 until foundRowGroupOffsets.length).foreach { i =>
          foundRowGroupOffsets(i) = footer.getBlocks().get(i).getStartingPos()
        }
        // this should never happen.
        // provide a good error message in case there's a bug
        throw new IllegalStateException(
          s"""All the offsets listed in the split should be found in the file.
            expected: $rowGroupOffsets
            found: $blocks
            out of: $foundRowGroupOffsets
             in range ${split.getStart}, ${split.getEnd}""")
      }
      (blocks, footer)
    }

    if (!blocks.isEmpty) {
      checkDeltaByteArrayProblem(footer.getFileMetaData(), configuration, blocks(0))
    }

    val fileSchema: MessageType = footer.getFileMetaData.getSchema

    internalReader = InternalParquetRecordReader(
      readSupport,
      fileSchema,
      footer.getFileMetaData,
      path,
      blocks,
      configuration)
  }

  private[this] def checkDeltaByteArrayProblem(meta: FileMetaData, conf: Configuration, block: BlockMetaData) {
    // splitting files?
    if (conf.getBoolean(ParquetInputFormat.SPLIT_FILES, true)) {
      // this is okay if not using DELTA_BYTE_ARRAY with the bug
      val encodings: JSet[Encoding] = new HashSet[Encoding]()
      block.getColumns.asScala.foreach{ column: ColumnChunkMetaData =>
        encodings.addAll(column.getEncodings())
      }
      encodings.asScala.foreach { encoding =>
        if (CorruptDeltaByteArrays.requiresSequentialReads(meta.getCreatedBy, encoding)) {
          throw new ParquetDecodingException("Cannot read data due to " +
            "PARQUET-246: to read safely, set " + SPLIT_FILES + " to false")
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  override def nextKeyValue(): Boolean = internalReader.nextKeyValue()

  private[this] def toParquetSplit(split: InputSplit): ParquetInputSplit =
    split match {
      case pis: ParquetInputSplit => pis
      case fs: FileSplit => ParquetInputSplitBridge.from(fs)
      case ofs: org.apache.hadoop.mapred.FileSplit => ParquetInputSplitBridge.from(ofs)
      case _ => throw new IllegalArgumentException("Invalid split (not a FileSplit or ParquetInputSplit): " + split)
    }
}
