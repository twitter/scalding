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

import java.lang.Boolean.TRUE
import java.util.Arrays.asList

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.util.{ List => JList, Map => JMap, Set => JSet }

import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter

import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.ParquetInputSplit
import scala.collection.JavaConverters._
import org.apache.parquet.hadoop.mapred.Container

object MapRedParquetInputFormat {

  private[MapRedParquetInputFormat] class ParquetInputSplitWrapper(var realSplit: ParquetInputSplit) extends InputSplit {

    def this() = this(null)

    override def getLength: Long = realSplit.getLength

    override def getLocations: Array[String] = realSplit.getLocations

    override def readFields(in: DataInput) {
      realSplit = new ParquetInputSplit()
      realSplit.readFields(in)
    }

    override def write(out: DataOutput) {
      realSplit.write(out)
    }
  }

  private[MapRedParquetInputFormat] object RecordReaderWrapper {
    def apply[V](oldSplit: InputSplit, oldJobConf: JobConf, reporter: Reporter): RecordReaderWrapper[V] =
      try {
        val realReader = new ParquetRecordReader[V](ParquetInputFormat.getReadSupportInstance[V](oldJobConf))

        oldSplit match {
          case pisq: ParquetInputSplitWrapper => realReader.initialize(pisq.realSplit, oldJobConf, reporter)
          case fs: FileSplit => realReader.initialize(fs, oldJobConf, reporter)
          case _ =>
            throw new IllegalArgumentException("Invalid split (not a FileSplit or ParquetInputSplitWrapper): " + oldSplit)
        }

        // read once to gain access to key and value objects
        val (firstRecord, valueContainer, eof) = if (realReader.nextKeyValue()) {
          val valueContainer = new Container[V]()
          valueContainer.set(realReader.getCurrentValue())
          (true, valueContainer, false)
        } else {
          (false, null, true)
        }

        RecordReaderWrapper[V](
          realReader,
          oldSplit.getLength,
          valueContainer,
          firstRecord,
          eof)

      } catch {
        case e: InterruptedException =>
          Thread.interrupted()
          throw new IOException(e)
      }
  }

  private[MapRedParquetInputFormat] case class RecordReaderWrapper[V] private (
    realReader: ParquetRecordReader[V],
    splitLen: Long,
    private val valueContainer: Container[V],
    private var firstRecord: Boolean,
    private var eof: Boolean) extends RecordReader[Void, Container[V]] {

    override def close() {
      realReader.close()
    }

    override def createKey(): Void =
      null

    override def createValue: Container[V] =
      valueContainer

    override def getPos: Long =
      (splitLen * getProgress()).toLong

    override def getProgress: Float =
      try {
        realReader.getProgress
      } catch {
        case e: InterruptedException =>
          Thread.interrupted()
          throw new IOException(e)
      }

    override def next(key: Void, value: Container[V]): Boolean = {
      if (eof) {
        return false
      }

      if (firstRecord) { // key & value are already read.
        firstRecord = false
        return true
      }

      try {
        if (realReader.nextKeyValue()) {
          if (value != null) value.set(realReader.getCurrentValue())
          return true
        }
      } catch {
        case e: InterruptedException =>
          throw new IOException(e)
      }

      eof = true; // strictly not required, just for consistency
      return false
    }
  }
}

class MapRedParquetInputFormat[V] extends org.apache.hadoop.mapred.FileInputFormat[Void, Container[V]] {
  import MapRedParquetInputFormat._
  protected val realInputFormat: ParquetInputFormat[V] = new ParquetInputFormat[V]()

  override def getRecordReader(split: InputSplit, job: JobConf,
    reporter: Reporter): RecordReader[Void, Container[V]] =
    RecordReaderWrapper[V](split, job, reporter)

  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    if (isTaskSideMetaData(job)) {
      return super.getSplits(job, numSplits)
    }

    val footers: JList[Footer] = getFooters(job)
    val splits: JList[ParquetInputSplit] = realInputFormat.getSplits(job, footers)
    if (splits == null) {
      return null
    }
    splits.asScala.map { split: ParquetInputSplit =>
      new ParquetInputSplitWrapper(split)
    }.toArray
  }

  def getFooters(job: JobConf): JList[Footer] =
    realInputFormat.getFooters(job, super.listStatus(job).toList.asJava)

  def isTaskSideMetaData(job: JobConf): Boolean =
    job.getBoolean(ParquetInputFormat.TASK_SIDE_METADATA, TRUE)
}
