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

//Wrapper of hdfs blocks, keep track of which HDFS block is being used

private[inputformat] object HDFSBlocks {

  def apply(hdfsBlocks: Array[BlockLocation]): HDFSBlocks = {
    val comparator = new Comparator[BlockLocation]() {
      override def compare(b1: BlockLocation, b2: BlockLocation): Int =
        java.lang.Long.signum(b1.getOffset() - b2.getOffset())
    }
    Arrays.sort(hdfsBlocks, comparator)
    new HDFSBlocks(hdfsBlocks)
  }
}

private[inputformat] class HDFSBlocks private (hdfsBlocks: Array[BlockLocation]) {
  var currentStartHdfsBlockIndex: Int = 0; //the hdfs block index corresponding to the start of a row group
  var currentMidPointHDFSBlockIndex: Int = 0; // the hdfs block index corresponding to the mid-point of a row group, a split might be created only when the midpoint of the rowgroup enters a new hdfs block

  private[this] def getHDFSBlockEndingPosition(hdfsBlockIndex: Int): Long = {
    val hdfsBlock = hdfsBlocks(hdfsBlockIndex)
    hdfsBlock.getOffset + hdfsBlock.getLength - 1
  }

  /**
   * @param rowGroupMetadata
   * @return true if the mid point of row group is in a new hdfs block, and also move the currentHDFSBlock pointer to the correct index that contains the row group
   * return false if the mid point of row group is in the same hdfs block
   */
  def checkBelongingToANewHDFSBlock(rowGroupMetadata: BlockMetaData): Boolean = {
    var isNewHdfsBlock: Boolean = false
    val rowGroupMidPoint: Long = rowGroupMetadata.getStartingPos() + (rowGroupMetadata.getCompressedSize() / 2)

    //if mid point is not in the current HDFS block any more, return true
    while (rowGroupMidPoint > getHDFSBlockEndingPosition(currentMidPointHDFSBlockIndex)) {
      isNewHdfsBlock = true
      currentMidPointHDFSBlockIndex += 1
      if (currentMidPointHDFSBlockIndex >= hdfsBlocks.length)
        throw new ParquetDecodingException("the row group is not in hdfs blocks in the file: midpoint of row groups is "
          + rowGroupMidPoint
          + ", the end of the hdfs block is "
          + getHDFSBlockEndingPosition(currentMidPointHDFSBlockIndex - 1))
    }

    while (rowGroupMetadata.getStartingPos() > getHDFSBlockEndingPosition(currentStartHdfsBlockIndex)) {
      currentStartHdfsBlockIndex += 1
      if (currentStartHdfsBlockIndex >= hdfsBlocks.length)
        throw new ParquetDecodingException("The row group does not start in this file: row group offset is "
          + rowGroupMetadata.getStartingPos()
          + " but the end of hdfs blocks of file is "
          + getHDFSBlockEndingPosition(currentStartHdfsBlockIndex))
    }
    isNewHdfsBlock
  }

  def getCurrentBlock: BlockLocation = hdfsBlocks(currentStartHdfsBlockIndex)
}