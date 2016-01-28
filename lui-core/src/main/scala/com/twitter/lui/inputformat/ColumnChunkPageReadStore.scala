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

import java.io.IOException

import org.apache.parquet.Ints
import org.apache.parquet.Log
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.column.page.PageReader
import org.apache.parquet.io.ParquetDecodingException

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 *
 */
private[inputformat] object ColumnChunkPageReadStore {
  private val LOG: Log = Log.getLog(classOf[ColumnChunkPageReadStore])
}

private[inputformat] class ColumnChunkPageReadStore(rowCount: Long) extends PageReadStore {
  import ColumnChunkPageReadStore._

  private[this] var readers = Map[ColumnDescriptor, ColumnChunkPageReader]()

  override def getRowCount(): Long = rowCount

  override def getPageReader(path: ColumnDescriptor): PageReader =

    readers.get(path).getOrElse {
      throw new IllegalArgumentException(s"$path is not in the store: ${readers.keys.mkString(",")} for $rowCount")
    }

  def addColumn(path: ColumnDescriptor, reader: ColumnChunkPageReader) {
    if (readers.contains(path)) {
      throw new RuntimeException(path + " was added twice")
    }
    readers = readers + (path -> reader)
  }

}
