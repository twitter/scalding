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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import cascading.flow.FlowConnector
import cascading.flow.hadoop.HadoopFlowConnector
import cascading.flow.local.LocalFlowConnector
import cascading.pipe.Pipe

import scala.collection.JavaConversions._
import cascading.tuple.Tuple
import collection.mutable.Buffer
import collection.mutable.{Map => MMap}

object Mode {
  /**
  * This mode is used by default by sources in read and write
  */
  implicit var mode : Mode = Local(false)
}
/**
* There are three ways to run jobs
* sourceStrictness is set to true
*/
abstract class Mode(val sourceStrictness : Boolean) {
  //We can't name two different pipes with the same name.
  protected val sourceMap = MMap[Source, Pipe]()

  def newFlowConnector(props : Map[AnyRef,AnyRef]) : FlowConnector

  /**
  * Cascading can't handle multiple head pipes with the same
  * name.  This handles them by caching the source and only
  * having a single head pipe to represent each head.
  */
  def getReadPipe(s : Source, p: => Pipe) : Pipe = {
    sourceMap.getOrElseUpdate(s, p)
  }

  def getSourceNamed(name : String) : Option[Source] = {
    sourceMap.find { _._1.toString == name }.map { _._1 }
  }

  // Returns true if the file exists on the current filesystem.
  def fileExists(filename : String) : Boolean
}

trait HadoopMode extends Mode {
  // config is iterable, but not a map, convert to one:
  implicit def configurationToMap(config : Configuration) = {
    config.foldLeft(Map[AnyRef, AnyRef]()) {
      (acc, kv) => acc + ((kv.getKey, kv.getValue))
    }
  }

  def jobConf : Configuration

  /*
   * for each key, do a set union of values, keeping the order from prop1 to prop2
   */
  protected def unionValues(prop1 : Map[AnyRef,AnyRef], prop2 : Map[AnyRef,AnyRef]) = {
    (prop1.keys ++ prop2.keys).foldLeft(Map[AnyRef,AnyRef]()) { (acc, key) =>
      val values1 = prop1.get(key).map { _.toString.split(",") }.getOrElse(Array[String]())
      val values2 = prop2.get(key).map { _.toString.split(",") }.getOrElse(Array[String]())
      //Only keep the different ones:
      val union = (values1 ++ values2.filter { !values1.contains(_) }).mkString(",")
      acc + ((key, union))
    }
  }

  def newFlowConnector(props : Map[AnyRef,AnyRef]) = {
    new HadoopFlowConnector(unionValues(jobConf, props))
  }
}

// Mix-in trait for test modes; overrides fileExists to allow the registration
// of mock filenames for testing.
trait TestMode extends Mode {
  private var fileSet = Set[String]()
  def registerTestFiles(files : Set[String]) = fileSet = files
  override def fileExists(filename : String) : Boolean = fileSet.contains(filename)
}

case class Hdfs(strict : Boolean, val config : Configuration) extends Mode(strict) with HadoopMode {
  override def jobConf = config
  override def fileExists(filename : String) : Boolean = {
    val filePath = new Path(filename)
    filePath.getFileSystem(config).exists(filePath)
  }
}

case class HadoopTest(val config : Configuration, val buffers : Map[Source,Buffer[Tuple]])
    extends Mode(false) with HadoopMode with TestMode {
  override def jobConf = config
}

case class Local(strict : Boolean) extends Mode(strict) {
  def newFlowConnector(props : Map[AnyRef,AnyRef]) = new LocalFlowConnector(props)
  override def fileExists(filename : String) : Boolean = new File(filename).exists
}

/**
* Memory only testing for unit tests
*/
case class Test(val buffers : Map[Source,Buffer[Tuple]]) extends Mode(false) with TestMode {
  def newFlowConnector(props : Map[AnyRef,AnyRef]) = new LocalFlowConnector(props)
}
