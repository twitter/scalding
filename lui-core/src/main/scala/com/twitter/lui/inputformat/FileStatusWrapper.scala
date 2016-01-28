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
import org.apache.parquet.hadoop.metadata.GlobalMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.ConfigurationUtil
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.util.HiddenFileFilter
import org.apache.parquet.hadoop.util.SerializationUtil
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.hadoop.ParquetInputSplit
import org.apache.parquet.hadoop.BadConfigurationException
import scala.collection.JavaConverters._
import org.apache.parquet.hadoop.Footer

case class FileStatusWrapper(fileStatus: FileStatus) {
  def getModificationTime: Long = fileStatus.getModificationTime
  override def hashCode = fileStatus.hashCode

  override def equals(other: Any): Boolean =
    other match {
      case fsW: FileStatusWrapper if fsW.fileStatus.equals(fileStatus) => true
      case _ => false
    }

  override def toString: String = fileStatus.getPath.toString
}