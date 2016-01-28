package com.twitter.lui.inputformat

import java.lang.Boolean.TRUE
import org.apache.parquet.Preconditions.checkArgument

import java.io.IOException
import java.util.ArrayList
import java.util.Arrays
import java.util.Collection
import java.util.Collections
import java.util.Comparator
import java.util.HashMap
import java.util.HashSet
import java.util.{ List => JList, Map => JMap, Set => JSet }

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.parquet.Log
import org.apache.parquet.Preconditions
import org.apache.parquet.filter.UnboundRecordFilter
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.filter2.compat.RowGroupFilter
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.api.InitContext
import com.twitter.lui.hadoop.ReadSupport
import com.twitter.lui.hadoop.ReadContext
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.ConfigurationUtil
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.util.HiddenFileFilter
import org.apache.parquet.hadoop.util.SerializationUtil
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.hadoop.ParquetInputSplit
import scala.collection.JavaConverters._

private[inputformat] class SplitInfo(hdfsBlock: BlockLocation) {
  private[this] val rowGroups = new ArrayList[BlockMetaData]()
  var compressedByteSize: Long = 0L

  def addRowGroup(rowGroup: BlockMetaData) {
    rowGroups.add(rowGroup)
    compressedByteSize += rowGroup.getCompressedSize()
  }

  def getCompressedByteSize: Long = compressedByteSize

  def getRowGroups: JList[BlockMetaData] = rowGroups

  def getRowGroupCount: Int = rowGroups.size()

  def getParquetInputSplit(fileStatus: FileStatus,
    requestedSchema: String,
    readSupportMetadata: Map[String, String]): ParquetInputSplit = {

    val requested: MessageType = MessageTypeParser.parseMessageType(requestedSchema)

    val length: Long = getRowGroups.asScala.flatMap { block: BlockMetaData =>
      block.getColumns.asScala.map { column =>
        if (requested.containsPath(column.getPath.toArray())) {
          column.getTotalSize
        } else 0L
      }
    }.sum

    val lastRowGroup: BlockMetaData = getRowGroups.get(this.getRowGroupCount - 1)
    val end: Long = lastRowGroup.getStartingPos + lastRowGroup.getTotalByteSize

    val rowGroupOffsets = rowGroups.asScala.map(_.getStartingPos).toArray

    new ParquetInputSplit(
      fileStatus.getPath,
      hdfsBlock.getOffset,
      end,
      length,
      hdfsBlock.getHosts,
      rowGroupOffsets)
  }
}
