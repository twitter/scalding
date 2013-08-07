/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import java.io.File
import java.util.{Map => JMap, UUID, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf

import cascading.flow.{FlowConnector, FlowDef, Flow}
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.flow.hadoop.HadoopFlowConnector
import cascading.flow.local.LocalFlowConnector
import cascading.flow.local.LocalFlowProcess
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.TupleEntryIterator

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{Set => MSet}
import scala.collection.mutable.Iterable

object Mode {
  /**
  * This mode is used by default by sources in read and write
  */
  protected val modeMap = MMap[String, Mode]()
  val MODE_KEY = "scalding.job.mode"

  // Map the specific mode to Job's UUID
  def putMode(mode : Mode, args : Args) : Args = synchronized {
    // Create Mode Id for the job
    val modeId = UUID.randomUUID
    val newArgs = args + (MODE_KEY -> List(modeId.toString))
    modeMap.put(newArgs(MODE_KEY), mode)
    newArgs
  }

  // Get the specific mode by UUID
  def getMode(args : Args) : Option[Mode] = synchronized {
    modeMap.get(args(MODE_KEY))
  }

  // This should be passed ALL the args supplied after the job name
  def apply(args : Args, config : Configuration) : Mode = {
    val strictSources = args.boolean("tool.partialok") == false
    if (!strictSources) {
      // TODO we should do smarter logging here
      println("[Scalding:INFO] using --tool.partialok. Missing log data won't cause errors.")
    }

    if (args.boolean("local"))
       Local(strictSources)
    else if (args.boolean("hdfs"))
      Hdfs(strictSources, config)
    else
      sys.error("[ERROR] Mode must be one of --local or --hdfs, you provided neither")
  }
}

trait Mode extends java.io.Serializable {
  /**
   * This is the input config of arguments passed in from Hadoop/Java
   * this map is transformed by Job.config before running
   */
  def config: Map[AnyRef, AnyRef]
  /*
   * Using a new FlowProcess, which is only suitable for reading outside
   * of a map/reduce job, open a given tap and return the TupleEntryIterator
   */
  def openForRead(tap : Tap[_,_,_]) : TupleEntryIterator
  // Returns true if the file exists on the current filesystem.
  def fileExists(filename : String) : Boolean
  /** Create a new FlowConnector for this cascading planner */
  def newFlowConnector(props : Map[AnyRef,AnyRef]): FlowConnector
}

trait HadoopMode extends Mode {
  def jobConf : Configuration

  override def config =
    jobConf.asScala.foldLeft(Map[AnyRef, AnyRef]()) {
      (acc, kv) => acc + ((kv.getKey, kv.getValue))
    }

  override def newFlowConnector(props : Map[AnyRef,AnyRef]) =
    new HadoopFlowConnector(props.asJava)

  // TODO  unlike newFlowConnector, this does not look at the Job.config
  override def openForRead(tap : Tap[_,_,_]) = {
    val htap = tap.asInstanceOf[Tap[JobConf,_,_]]
    val conf = new JobConf(jobConf)
    val fp = new HadoopFlowProcess(conf)
    htap.retrieveSourceFields(fp)
    htap.sourceConfInit(fp, conf)
    htap.openForRead(fp)
  }
}

trait CascadingLocal extends Mode {
  override def config = Map[AnyRef, AnyRef]()

  override def newFlowConnector(props : Map[AnyRef,AnyRef]) =
    new LocalFlowConnector(props.asJava)

  override def openForRead(tap : Tap[_,_,_]) = {
    val ltap = tap.asInstanceOf[Tap[Properties,_,_]]
    val fp = new LocalFlowProcess
    ltap.retrieveSourceFields(fp)
    ltap.openForRead(fp)
  }
}

// Mix-in trait for test modes; overrides fileExists to allow the registration
// of mock filenames for testing.
trait TestMode extends Mode {
  private var fileSet = Set[String]()
  def registerTestFiles(files : Set[String]) = fileSet = files
  override def fileExists(filename : String) : Boolean = fileSet.contains(filename)
}

case class Hdfs(strict : Boolean, conf : Configuration) extends HadoopMode {
  override def jobConf = conf
  override def fileExists(filename : String) : Boolean =
    FileSystem.get(jobConf).exists(new Path(filename))
}

case class HadoopTest(conf : Configuration, buffers : Map[Source,Buffer[Tuple]])
    extends HadoopMode with TestMode {

  // This is a map from source.toString to disk path
  private val writePaths = MMap[Source, String]()
  private val allPaths = MSet[String]()

  override def jobConf = conf

  @tailrec
  private def allocateNewPath(prefix : String, idx : Int) : String = {
    val candidate = prefix + idx.toString
    if (allPaths(candidate)) {
      //Already taken, try again:
      allocateNewPath(prefix, idx + 1)
    }
    else {
      // Update all paths:
      allPaths += candidate
      candidate
    }
  }

  private val basePath = "/tmp/scalding/"
  // Looks up a local path to write the given source to
  def getWritePathFor(src : Source) : String = {
    val rndIdx = new java.util.Random().nextInt(1 << 30)
    writePaths.getOrElseUpdate(src, allocateNewPath(basePath + src.getClass.getName, rndIdx))
  }

  def finalize(src : Source) {
    // Get the buffer for the given source, and empty it:
    val buf = buffers(src)
    buf.clear()
    // Now fill up this buffer with the content of the file
    val path = getWritePathFor(src)
    // We read the write tap in order to add its contents in the test buffers
    val it = openForRead(src.createTap(Write)(this))
    while(it != null && it.hasNext) {
      buf += new Tuple(it.next.getTuple)
    }
    //Clean up this data off the disk
    new File(path).delete()
    writePaths -= src
  }
}

case class Local(strictSources: Boolean) extends CascadingLocal {
  override def fileExists(filename : String) : Boolean = new File(filename).exists
}

/**
* Memory only testing for unit tests
*/
case class Test(val buffers : Map[Source,Buffer[Tuple]]) extends TestMode with CascadingLocal
