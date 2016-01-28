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
package com.twitter.lui.inputformat

import org.apache.parquet.bytes.BytesUtils.readIntLittleEndian
import org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics

import java.io.ByteArrayInputStream
import java.io.Closeable
import java.io.IOException
import java.io.SequenceInputStream
import java.util.Arrays
import java.util.Collection
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.parquet.Log
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.DataPageV2
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.format.DataPageHeader
import org.apache.parquet.format.DataPageHeaderV2
import org.apache.parquet.format.DictionaryPageHeader
import org.apache.parquet.format.PageHeader
import org.apache.parquet.format.Util
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HiddenFileFilter
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.hadoop.metadata.FileMetaData
import com.twitter.lui.inputformat.codec.{ CodecFactory, BytesDecompressor }
import scala.collection.JavaConverters._

import scala.collection.mutable.{ Map => MMap }

/**
 * information needed to read a column chunk
 */
private[inputformat] case class ChunkDescriptor(
  col: ColumnDescriptor,
  metadata: ColumnChunkMetaData,
  fileOffset: Long,
  size: Int)

object ParquetFileReader {
  private val LOG: Log = Log.getLog(classOf[ParquetFileReader])
  private val converter: ParquetMetadataConverter = new ParquetMetadataConverter()

}

class ParquetFileReader(configuration: Configuration,
  fileMetadata: FileMetaData,
  filePath: Path,
  blocks: IndexedSeq[BlockMetaData],
  columns: List[ColumnDescriptor]) extends Closeable {
  import ParquetFileReader._

  val createdBy = if (fileMetadata == null) null else fileMetadata.getCreatedBy

  private[this] val codecFactory: CodecFactory = new CodecFactory(configuration)

  private[this] val f: FSDataInputStream = {
    val fs: FileSystem = filePath.getFileSystem(configuration)
    fs.open(filePath)
  }

  private[this] val paths: Map[ColumnPath, ColumnDescriptor] = columns.map { col =>
    ColumnPath.get(col.getPath: _*) -> col
  }.toMap

  private[this] var currentBlock: Int = 0

  /**
   * Reads all the columns requested from the row group at the current file position.
   * @throws IOException if an error occurs while reading
   * @return the PageReadStore which can provide PageReaders for each column.
   */
  def readNextRowGroup(): PageReadStore = {
    if (currentBlock == blocks.size) {
      null
    } else {
      val block: BlockMetaData = blocks(currentBlock)
      if (block.getRowCount == 0) {
        throw new RuntimeException("Illegal row group of 0 rows")
      }

      val columnChunkPageReadStore: ColumnChunkPageReadStore = new ColumnChunkPageReadStore(block.getRowCount)

      block.getColumns.asScala.foreach { mc =>
        val columnDescriptor = paths(mc.getPath)
        val descriptor = ChunkDescriptor(columnDescriptor, mc, mc.getStartingPos, mc.getTotalSize.asInstanceOf[Int])

        columnChunkPageReadStore.addColumn(descriptor.col, new ColumnChunkPageReader(ColumnChunkPreStaged(descriptor, f, codecFactory, createdBy)))
      }
      currentBlock += 1
      columnChunkPageReadStore
    }
  }

  override def close() {
    f.close()
    codecFactory.release()
  }
}
