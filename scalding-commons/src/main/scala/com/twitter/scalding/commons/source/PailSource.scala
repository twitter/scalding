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

package com.twitter.scalding.commons.source

import com.backtype.cascading.tap.PailTap
import com.backtype.hadoop.pail.{Pail, PailStructure}
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import com.twitter.bijection.Injection
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import java.util.{ List => JList }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import scala.collection.JavaConverters._

/**
 * The PailSource enables scalding integration with the Pail class in the
 * dfs-datastores library. PailSource allows scalding to sink 1-tuples
 * to subdirectories of a root folder by applying a routing function to
 * each tuple.
 *
 * SEE EXAMPLE : https://gist.github.com/krishnanraman/5224937
 */

object PailSource {

 /**
  * the simplest version of sink - THE MOST COMMON USE CASE
  * specify exactly 2 parameters
  * rootPath - the location ie. Where do you want your Pail to reside ?
  * targetFn - the partition function ie. How do we create Pail subdirectories out of your input space ?
  *
  * SEE EXAMPLE : https://gist.github.com/krishnanraman/5224937
  */
  def sink[T]( rootPath: String,
               targetFn: (T) => List[String] )
              (implicit cmf: ClassManifest[T],
               injection: Injection[T, Array[Byte]]):PailSource[T] = {

    val validator = ((x:List[String])=> true)
    val cps = new CodecPailStructure[T]()
    cps.setParams( targetFn, validator, cmf.erasure.asInstanceOf[Class[T]], injection)
    sink(rootPath, cps)
 }

 /**
  * the simplest version of source - THE MOST COMMON USE CASE
  * specify exactly 2 parameters
  * rootPath - the location ie. Where does your Pail reside - its root directory ?
  * subPath - the location ie. Where does your Pail reside - its subdirectories ?
  * eg. Say your data resides in foo/bar, foo/obj, foo/ghj
  * If you care about obj & ghj, the rootPath = "foo", subPaths = Array(List("obj"), List("ghj"))
  * Notice that subPaths != Array(List("obj", "ghj")) - this would fail.
  * Every subdirectory goes in its own list.
  *
  * SEE EXAMPLE : https://gist.github.com/krishnanraman/5224937
  */
  def source[T](rootPath: String,
                subPaths: Array[List[String]])
              (implicit cmf: ClassManifest[T],
               injection: Injection[T, Array[Byte]]):PailSource[T] = {

    val validator = ((x:List[String])=> true)
    val cps = new CodecPailStructure[T]()
    cps.setParams( null, validator, cmf.erasure.asInstanceOf[Class[T]], injection)
    source( rootPath, cps, subPaths)
  }

  /** Generic version of Pail sink accepts a PailStructure.
  */
  def sink[T](rootPath: String, structure: PailStructure[T]):PailSource[T] =
    new PailSource(rootPath, structure)

  /** A Pail sink can also build its structure on the fly from a
  *   couple of functions.
  */
  def sink[T]( rootPath: String,
                targetFn: (T) => List[String],
                validator: (List[String]) => Boolean,
                mytype:java.lang.Class[T],
                injection: Injection[T, Array[Byte]]):PailSource[T] = {

    val cps = new CodecPailStructure[T]()
    cps.setParams( targetFn, validator, mytype, injection)
    sink( rootPath, cps)
  }

  /** Alternate sink construction
  *   Using implicit injections & classmanifest for the type
  */
  def sink[T]( rootPath: String,
               targetFn: (T) => List[String],
               validator: (List[String]) => Boolean)
              (implicit cmf: ClassManifest[T],
               injection: Injection[T, Array[Byte]]):PailSource[T] = {
    val cps = new CodecPailStructure[T]()
    cps.setParams( targetFn, validator, cmf.erasure.asInstanceOf[Class[T]], injection)
    sink(rootPath, cps)
 }

  /** Generic version of Pail source accepts a PailStructure.
  */
  def source[T](rootPath: String, structure: PailStructure[T], subPaths: Array[List[String]]):PailSource[T] = {
    assert( subPaths != null && subPaths.size > 0)
    new PailSource(rootPath, structure, subPaths)
  }

  /** The most explicit method to construct a Pail source - specify all 5 params
  */
  def source[T](rootPath: String,
                validator: (List[String]) => Boolean,
                mytype:java.lang.Class[T],
                injection: Injection[T, Array[Byte]] ,
                subPaths: Array[List[String]]):PailSource[T] = {
    val cps = new CodecPailStructure[T]()
    cps.setParams( null, validator, mytype, injection)
    source( rootPath, cps, subPaths)
  }

  /** Alternate Pail source construction - specify 3 params, rest implicit
  */
  def source[T](rootPath: String,
                validator: (List[String]) => Boolean,
                subPaths: Array[List[String]])
              (implicit cmf: ClassManifest[T],
                injection: Injection[T, Array[Byte]]):PailSource[T] = {
    val cps = new CodecPailStructure[T]()
    cps.setParams( null, validator, cmf.erasure.asInstanceOf[Class[T]], injection)
    source( rootPath, cps, subPaths)
  }
}

class PailSource[T] private (rootPath: String, structure: PailStructure[T], subPaths: Array[List[String]] = null)
extends Source with Mappable[T] {
  import Dsl._

  override val converter = singleConverter[T]
  val fieldName = "pailItem"

  lazy val getTap = {
    val spec = PailTap.makeSpec(null, structure)
    val javaSubPath = if ((subPaths == null) || (subPaths.size == 0)) null else subPaths map { _.asJava }
    val opts = new PailTap.PailTapOptions(spec, fieldName, javaSubPath , null)
    new PailTap(rootPath, opts)
  }

  override def hdfsScheme = getTap.getScheme
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[Object], Array[Object]]]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tap = castHfsTap(getTap)

    mode match {
      case Hdfs(strict, config) =>
        readOrWrite match {
          case Read  => tap
          case Write => tap
        }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

}

/**
  * It is quite unlikely for client code to make a CodecPailStructure
  * CodecPailStructure is constructed by PailSource's factory methods.
  *
  * targetFn takes an instance of T and returns a list
  *"path components". Pail joins these components with
  * File.separator and sinks the instance of T into the pail at that location.
  *
  * Usual implementations of "validator" will check that the length of
  * the supplied list is >= the length f the list returned by targetFn.
  *
  * CodecPailStructure has a default constructor because it is instantiated via reflection
  * This unfortunately means params must be set via setParams to make it usefuls
*/

class CodecPailStructure[T] extends PailStructure[T] {

  private var targetFn: T => List[String] = null
  private var validator :List[String] => Boolean = ((x:List[String])=> true)
  private var mytype: java.lang.Class[T] = null
  private var injection: Injection[T, Array[Byte]] = null

  private[source] def setParams(  targetFn: T => List[String],
                  validator: List[String] => Boolean,
                  mytype:java.lang.Class[T],
                  injection: Injection[T, Array[Byte]]) = {

    this.targetFn = targetFn
    this.validator = validator
    this.mytype = mytype
    this.injection = injection
  }
  override def isValidTarget(paths: String*): Boolean = validator(paths.toList)
  override def getTarget(obj: T): JList[String] = targetFn(obj).toList.asJava
  override def serialize(obj: T): Array[Byte] = injection.apply(obj)
  override def deserialize(bytes: Array[Byte]): T = injection.invert(bytes).get
  override val getType = mytype
}

