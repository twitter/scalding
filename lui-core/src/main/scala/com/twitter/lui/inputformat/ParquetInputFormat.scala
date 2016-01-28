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
import org.apache.parquet.hadoop.{ ParquetInputSplit, ParquetInputSplitBridge }
import org.apache.parquet.hadoop.BadConfigurationException
import scala.collection.JavaConverters._
import org.apache.parquet.hadoop.Footer
import scala.reflect.ClassTag
/**
 * The input format to read a Parquet file.
 *
 * It requires an implementation of {@link ReadSupport} to materialize the records.
 *
 * The requestedSchema will control how the original records get projected by the loader.
 * It must be a subset of the original schema. Only the columns needed to reconstruct the records with the requestedSchema will be scanned.
 *
 * @see #READ_SUPPORT_CLASS
 * @see #UNBOUND_RECORD_FILTER
 * @see #STRICT_TYPE_CHECKING
 * @see #FILTER_PREDICATE
 * @see #TASK_SIDE_METADATA
 *
 * @param [T] the type of the materialized records
 */
object ParquetInputFormat {
  val LOG: Log = Log.getLog(getClass)

  /**
   * key to configure the ReadSupport implementation
   */
  val READ_SUPPORT_CLASS = "parquet.read.support.class"

  val TARGET_CLASS = "parquet.target.class"

  /**
   * key to configure the filter
   */
  val UNBOUND_RECORD_FILTER = "parquet.read.filter"

  /**
   * key to configure type checking for conflicting schemas (default: true)
   */
  val STRICT_TYPE_CHECKING = "parquet.strict.typing"

  /**
   * key to configure the filter predicate
   */
  val FILTER_PREDICATE = "parquet.private.read.filter.predicate"

  /**
   * key to turn on or off task side metadata loading (default true)
   * if true then metadata is read on the task side and some tasks may finish immediately.
   * if false metadata is read on the client which is slower if there is a lot of metadata but tasks will only be spawn if there is work to do.
   */
  val TASK_SIDE_METADATA = "parquet.task.side.metadata"

  /**
   * key to turn off file splitting. See PARQUET-246.
   */
  val SPLIT_FILES = "parquet.split.files"

  private val MIN_FOOTER_CACHE_SIZE = 100

  def setTaskSideMetaData(job: Job, taskSideMetadata: Boolean) {
    ContextUtil.getConfiguration(job).setBoolean(TASK_SIDE_METADATA, taskSideMetadata)
  }

  def isTaskSideMetaData(configuration: Configuration): Boolean =
    configuration.getBoolean(TASK_SIDE_METADATA, TRUE)

  def setReadSupportClass(conf: JobConf, readSupportClass: Class[_], targetClass: Class[_]) {
    conf.set(READ_SUPPORT_CLASS, readSupportClass.getName)
    conf.set(TARGET_CLASS, targetClass.getName)
  }

  def getClassFromConfig(configuration: Configuration, configName: String, assignableFromOpt: Option[Class[_]]): (String, Class[_]) = {
    val className = configuration.get(configName)
    if (className == null) {
      return null;
    }

    try {
      val foundClass = configuration.getClassByName(className)
      assignableFromOpt.foreach { assignableFrom =>
        if (!assignableFrom.isAssignableFrom(foundClass)) {
          throw new BadConfigurationException("class " + className + " set in job conf at "
            + configName + " is not a subclass of " + assignableFrom.getCanonicalName());
        }
      }
      (className, foundClass)
    } catch {
      case e: ClassNotFoundException =>
        throw new BadConfigurationException("could not instantiate class " + className + " set in job conf at " + configName, e);
    }
  }

  def getReadSupportClass[T](configuration: Configuration): (String, Class[_ <: ReadSupport[T]]) = {
    val (name, clazz) = getClassFromConfig(configuration, READ_SUPPORT_CLASS, Some(classOf[ReadSupport[_]]))

    (name, clazz.asInstanceOf[Class[ReadSupport[T]]])
  }

  def getTargetClass[T](configuration: Configuration): (String, Class[T]) = {
    val (name, clazz) = getClassFromConfig(configuration, TARGET_CLASS, None)

    (name, clazz.asInstanceOf[Class[T]])
  }

  def getReadSupportInstance[T](configuration: Configuration): ReadSupport[T] = {
    val (className, readSupportClass) = getReadSupportClass[T](configuration)
    try {
      val inst = readSupportClass.newInstance

      val (targetClassName, clazz) = getTargetClass(configuration)
      inst.setClassTag(ClassTag(clazz))
      inst

    } catch {
      case e: InstantiationException => throw new BadConfigurationException(s"could not instantiate read support class: $className", e)
      case e: IllegalAccessException => throw new BadConfigurationException(s"could not instantiate read support class: $className", e)
    }
  }

  private def getAllFileRecursively(
    files: JList[FileStatus], conf: Configuration): JList[FileStatus] = {
    val result: JList[FileStatus] = new ArrayList[FileStatus]()

    files.asScala.foreach { file =>
      if (file.isDir()) {
        val p: Path = file.getPath()
        val fs: FileSystem = p.getFileSystem(conf)
        staticAddInputPathRecursively(result, fs, p, HiddenFileFilter.INSTANCE)
      } else {
        result.add(file)
      }
    }
    LOG.info("Total input paths to process : " + result.size())
    result
  }

  private def staticAddInputPathRecursively(result: JList[FileStatus],
    fs: FileSystem, path: Path, inputFilter: PathFilter): Unit = {
    fs.listStatus(path, inputFilter).foreach { stat: FileStatus =>
      if (stat.isDir()) {
        staticAddInputPathRecursively(result, fs, stat.getPath(), inputFilter)
      } else {
        result.add(stat)
      }
    }
  }

  object FootersCacheValue {
    def apply(status: FileStatusWrapper, footer: Footer): FootersCacheValue =
      new FootersCacheValue(status.getModificationTime, new Footer(footer.getFile, footer.getParquetMetadata))
  }
  class FootersCacheValue(val modificationTime: Long, val footer: Footer) extends LruCache.Value[FileStatusWrapper, FootersCacheValue] {

    override def isCurrent(key: FileStatusWrapper): Boolean = {
      val currentModTime: Long = key.getModificationTime
      val isCurrent: Boolean = modificationTime >= currentModTime
      if (Log.DEBUG && !isCurrent) {
        LOG.debug("The cache value for '" + key + "' is not current: "
          + "cached modification time=" + modificationTime + ", "
          + "current modification time: " + currentModTime)
      }
      isCurrent
    }

    def getFooter: Footer = footer

    override def isNewerThan(otherValue: FootersCacheValue): Boolean =
      otherValue == null || modificationTime > otherValue.modificationTime

    def getPath: Path = footer.getFile
  }
}

class ParquetInputFormat[T] extends FileInputFormat[Void, T] {
  import ParquetInputFormat._

  private[this] var footersCache: LruCache[FileStatusWrapper, FootersCacheValue] = _

  /**
   * {@inheritDoc}
   */

  override def createRecordReader(
    inputSplit: InputSplit,
    taskAttemptContext: TaskAttemptContext): RecordReader[Void, T] = {
    val conf: Configuration = ContextUtil.getConfiguration(taskAttemptContext)
    val readSupport: ReadSupport[T] = getReadSupportInstance[T](conf)
    ParquetRecordReader[T](readSupport)
  }

  protected override def isSplitable(context: JobContext, filename: Path) =
    ContextUtil.getConfiguration(context).getBoolean(SPLIT_FILES, true)

  override def getSplits(jobContext: JobContext): JList[InputSplit] = {
    val configuration: Configuration = ContextUtil.getConfiguration(jobContext)
    val splits: JList[InputSplit] = new ArrayList[InputSplit]()

    if (isTaskSideMetaData(configuration)) {
      // Although not required by the API, some clients may depend on always
      // receiving ParquetInputSplit. Translation is required at some point.
      super.getSplits(jobContext).asScala.foreach{ split =>
        Preconditions.checkArgument(split.isInstanceOf[FileSplit],
          "Cannot wrap non-FileSplit: " + split)
        splits.add(ParquetInputSplitBridge.from(split.asInstanceOf[FileSplit]))
      }
      return splits
    } else {
      splits.addAll(getSplits(configuration, getFooters(jobContext)))
    }

    splits
  }

  def getSplits(configuration: Configuration, footers: JList[Footer]): JList[ParquetInputSplit] = {
    val strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true)
    val maxSplitSize = configuration.getLong("mapred.max.split.size", java.lang.Long.MAX_VALUE)
    val minSplitSize = Math.max(getFormatMinSplitSize(), configuration.getLong("mapred.min.split.size", 0L))
    if (maxSplitSize < 0 || minSplitSize < 0) {
      throw new ParquetDecodingException("maxSplitSize or minSplitSize should not be negative: maxSplitSize = " + maxSplitSize + "; minSplitSize = " + minSplitSize);
    }
    val globalMetaData: GlobalMetaData = org.apache.parquet.hadoop.ParquetFileWriterBridge.getGlobalMetaData(footers, strictTypeChecking)
    val readContext: ReadContext = getReadSupportInstance(configuration).init(new InitContext(
      configuration,
      globalMetaData.getKeyValueMetaData(),
      globalMetaData.getSchema()))

    new ClientSideMetadataSplitStrategy().getSplits(configuration, footers, maxSplitSize, minSplitSize, readContext)
  }

  /*
   * This is to support multi-level/recursive directory listing until
   * MAPREDUCE-1577 is fixed.
   */
  override protected def listStatus(jobContext: JobContext): JList[FileStatus] =
    getAllFileRecursively(super.listStatus(jobContext), ContextUtil.getConfiguration(jobContext))

  /**
   * @param jobContext the current job context
   * @return the footers for the files
   * @throws IOException
   */
  def getFooters(jobContext: JobContext): JList[Footer] = {
    val statuses: JList[FileStatus] = listStatus(jobContext)
    if (statuses.isEmpty()) {
      return Collections.emptyList()
    }
    val config: Configuration = ContextUtil.getConfiguration(jobContext)
    val footers: JList[Footer] = new ArrayList[Footer](statuses.size())
    val missingStatuses: JSet[FileStatus] = new HashSet[FileStatus]()
    val missingStatusesMap: JMap[Path, FileStatusWrapper] =
      new HashMap[Path, FileStatusWrapper](missingStatuses.size())

    if (footersCache == null) {
      footersCache = LruCache[FileStatusWrapper, FootersCacheValue](Math.max(statuses.size(), MIN_FOOTER_CACHE_SIZE))
    }
    statuses.asScala.foreach { status =>
      val statusWrapper: FileStatusWrapper = new FileStatusWrapper(status)
      val cacheEntry: FootersCacheValue = footersCache.getCurrentValue(statusWrapper)
      if (Log.DEBUG) {
        LOG.debug(s"""Cache entry ${if (cacheEntry == null) "not" else ""} found for '${status.getPath}'""")
      }
      if (cacheEntry != null) {
        footers.add(cacheEntry.getFooter)
      } else {
        missingStatuses.add(status)
        missingStatusesMap.put(status.getPath, statusWrapper)
      }
    }

    if (Log.DEBUG) {
      LOG.debug("found " + footers.size() + " footers in cache and adding up "
        + "to " + missingStatuses.size() + " missing footers to the cache")
    }

    if (missingStatuses.isEmpty()) {
      return footers
    }

    val newFooters: JList[Footer] = getFooters(config, missingStatuses)
    newFooters.asScala.foreach { newFooter =>
      // Use the original file status objects to make sure we store a
      // conservative (older) modification time (i.e. in case the files and
      // footers were modified and it's not clear which version of the footers
      // we have)
      val fileStatus: FileStatusWrapper = missingStatusesMap.get(newFooter.getFile())
      footersCache.put(fileStatus, FootersCacheValue(fileStatus, newFooter))
    }

    footers.addAll(newFooters)
    return footers
  }

  /**
   * the footers for the files
   * @param configuration to connect to the file system
   * @param statuses the files to open
   * @return the footers of the files
   * @throws IOException
   */
  def getFooters(configuration: Configuration, statuses: Collection[FileStatus]): JList[Footer] = {
    if (Log.DEBUG) LOG.debug("reading " + statuses.size() + " files")
    val taskSideMetaData = isTaskSideMetaData(configuration)
    org.apache.parquet.hadoop.ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses, taskSideMetaData)
  }

  /**
   * @param jobContext the current job context
   * @return the merged metadata from the footers
   * @throws IOException
   */
  def getGlobalMetaData(jobContext: JobContext): GlobalMetaData =
    org.apache.parquet.hadoop.ParquetFileWriterBridge.getGlobalMetaData(getFooters(jobContext))
}