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

import com.twitter.lui.inputformat.codec._
import java.io.ByteArrayInputStream
import java.io.IOException
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.page.PageReader
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics
import org.apache.parquet.format.DataPageHeader
import org.apache.parquet.format.DictionaryPageHeader
import org.apache.parquet.format.PageHeader
import org.apache.parquet.format.PageType
import org.apache.parquet.format.Util
import org.apache.parquet.Ints
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.Log

class CustomByteArrayInputStream(data: Array[Byte]) extends ByteArrayInputStream(data) {
  def getPos = pos
  def readPageHeader(): PageHeader = Util.readPageHeader(this)
  def readAsBytesInput(size: Int): BytesInput = {
    val r: BytesInput = BytesInput.from(buf, pos, size)
    pos += size
    r
  }
}

case class ColumnChunkPreStaged(descriptor: ChunkDescriptor, f: FSDataInputStream, codecFactory: CodecFactory, createdBy: String)

case class ColumnChunkData(var pagesInChunk: List[DataPage],
  dictionaryPage: DictionaryPage,
  decompressor: BytesDecompressor) {
  def popDataPage: DataPage = if (pagesInChunk.isEmpty) null else {
    val h = pagesInChunk.head
    pagesInChunk = pagesInChunk.tail
    h
  }
}

object ColumnChunkData {
  private val converter: ParquetMetadataConverter = new ParquetMetadataConverter()
  private val LOG: Log = Log.getLog(classOf[ColumnChunkData])

  def apply(preStaged: ColumnChunkPreStaged): ColumnChunkData = {
    val mc = preStaged.descriptor.metadata
    preStaged.f.seek(mc.getStartingPos)
    val totalSize = mc.getTotalSize.asInstanceOf[Int]
    val chunksBytes: Array[Byte] = new Array[Byte](totalSize)
    preStaged.f.readFully(chunksBytes)

    val bais = new CustomByteArrayInputStream(chunksBytes)
    val descriptor = preStaged.descriptor
    val createdBy = preStaged.createdBy
    val decompressor: BytesDecompressor = preStaged.codecFactory.getDecompressor(descriptor.metadata.getCodec)

    var pagesInChunk: List[DataPage] = List[DataPage]()
    var dictionaryPage: DictionaryPage = null
    var valuesCountReadSoFar: Long = 0

    while (valuesCountReadSoFar < descriptor.metadata.getValueCount) {
      val pageHeader: PageHeader = bais.readPageHeader
      val uncompressedPageSize: Int = pageHeader.getUncompressed_page_size
      val compressedPageSize: Int = pageHeader.getCompressed_page_size
      pageHeader.getType match {
        case PageType.DICTIONARY_PAGE =>
          // there is only one dictionary page per column chunk
          if (dictionaryPage != null) {
            throw new ParquetDecodingException(s"more than one dictionary page in column : ${descriptor.col}")
          }
          val dicHeader: DictionaryPageHeader = pageHeader.getDictionary_page_header
          dictionaryPage = new DictionaryPage(
            bais.readAsBytesInput(compressedPageSize),
            uncompressedPageSize,
            dicHeader.getNum_values,
            converter.getEncoding(dicHeader.getEncoding))
        case PageType.DATA_PAGE =>
          val dataHeaderV1: DataPageHeader = pageHeader.getData_page_header
          pagesInChunk = pagesInChunk :+ new DataPageV1(
            bais.readAsBytesInput(compressedPageSize),
            dataHeaderV1.getNum_values,
            uncompressedPageSize,
            fromParquetStatistics(createdBy, dataHeaderV1.getStatistics, descriptor.col.getType),
            converter.getEncoding(dataHeaderV1.getRepetition_level_encoding),
            converter.getEncoding(dataHeaderV1.getDefinition_level_encoding),
            converter.getEncoding(dataHeaderV1.getEncoding))
          valuesCountReadSoFar += dataHeaderV1.getNum_values

        case other =>
          LOG.warn(s"skipping page of type ${pageHeader.getType} of size ${compressedPageSize}")
          bais.skip(compressedPageSize)
      }
    }
    if (valuesCountReadSoFar != descriptor.metadata.getValueCount) {
      throw new IOException(
        s"""Expected ${descriptor.metadata.getValueCount} values in column chunk at
            | offset ${descriptor.metadata.getFirstDataPageOffset} but got
            | $valuesCountReadSoFar values instead over ${pagesInChunk.size}
            | pages ending at file offset ${descriptor.fileOffset + bais.getPos}""".stripMargin('|'))
    }
    ColumnChunkData(pagesInChunk,
      dictionaryPage,
      decompressor)
  }
}

private[inputformat] class ColumnChunkPageReader(prestagedData: ColumnChunkPreStaged) extends PageReader {

  lazy val columnChunkData: ColumnChunkData = ColumnChunkData(prestagedData)

  private[this] lazy val valueCount = columnChunkData.pagesInChunk.map(_.getValueCount).sum

  override def getTotalValueCount: Long = valueCount

  override def readPage(): DataPage =
    if (columnChunkData.pagesInChunk.isEmpty) {
      null
    } else {
      val compressedPage: DataPage = columnChunkData.popDataPage

      compressedPage match {
        case dataPageV1: DataPageV1 =>
          try {
            new DataPageV1(
              columnChunkData.decompressor.decompress(dataPageV1.getBytes(), dataPageV1.getUncompressedSize()),
              dataPageV1.getValueCount(),
              dataPageV1.getUncompressedSize(),
              dataPageV1.getStatistics(),
              dataPageV1.getRlEncoding(),
              dataPageV1.getDlEncoding(),
              dataPageV1.getValueEncoding())
          } catch {
            case e: IOException =>
              throw new ParquetDecodingException("could not decompress page", e)
          }
      }
    }

  override def readDictionaryPage(): DictionaryPage =
    if (columnChunkData.dictionaryPage == null) {
      null
    } else {
      try {
        val compressedDictionaryPage = columnChunkData.dictionaryPage
        new DictionaryPage(
          columnChunkData.decompressor.decompress(compressedDictionaryPage.getBytes(), compressedDictionaryPage.getUncompressedSize()),
          compressedDictionaryPage.getDictionarySize(),
          compressedDictionaryPage.getEncoding())
      } catch {

        case e: IOException =>
          throw new RuntimeException(e)
      }
    }
}