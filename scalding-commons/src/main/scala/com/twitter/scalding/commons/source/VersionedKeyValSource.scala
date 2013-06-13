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

import com.backtype.cascading.scheme.KeyValueByteScheme
import com.backtype.cascading.tap.VersionedTap
import com.backtype.cascading.tap.VersionedTap.TapMode
import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.local.TextDelimited
import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import com.twitter.scalding.source.{ CheckedInversion, MaxFailuresCheck }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

/**
 * Source used to write key-value pairs as byte arrays into a versioned store.
 * Supports incremental updates via the monoid on V.
 */

object VersionedKeyValSource {
  val defaultVersionsToKeep = 3

  // TODO: have two apply methods here for binary compatibility purpose. Need to clean it up in next release.
  def apply[K,V](path: String, sourceVersion: Option[Long] = None, sinkVersion: Option[Long] = None, maxFailures: Int = 0)
  (implicit codec: Injection[(K,V),(Array[Byte],Array[Byte])]) = {
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, maxFailures, defaultVersionsToKeep)
  }

  def apply[K,V](path: String, sourceVersion: Option[Long], sinkVersion: Option[Long], maxFailures: Int, versionsToKeep: Int)
  (implicit codec: Injection[(K,V),(Array[Byte],Array[Byte])]) =
    new VersionedKeyValSource[K,V](path, sourceVersion, sinkVersion, maxFailures, versionsToKeep)
}

class VersionedKeyValSource[K,V](val path: String, val sourceVersion: Option[Long], val sinkVersion: Option[Long],
  val maxFailures: Int, val versionsToKeep: Int)(
    implicit @transient codec: Injection[(K,V),(Array[Byte],Array[Byte])]) extends Source with Mappable[(K,V)] {

  import Dsl._

  val keyField = "key"
  val valField = "value"
  val fields = new Fields(keyField, valField)
  val codecBox = MeatLocker(codec)

  override def converter[U >: (K, V)] = TupleConverter.asSuperConverter[(K, V), U](TupleConverter.of[(K, V)])
  override def localScheme = new TextDelimited(fields)

  override def hdfsScheme =
    HadoopSchemeInstance(new KeyValueByteScheme(fields).asInstanceOf[Scheme[_, _, _, _, _]])

  @deprecated("This method is deprecated", "0.1.6")
  def this(path: String, sourceVersion: Option[Long], sinkVersion: Option[Long], maxFailures: Int)
    (implicit @transient codec: Injection[(K,V),(Array[Byte],Array[Byte])]) =
    this(path, sourceVersion, sinkVersion, maxFailures, VersionedKeyValSource.defaultVersionsToKeep)(codec)

  def getTap(mode: TapMode) = {
    val tap = new VersionedTap(path, hdfsScheme, mode).setVersionsToKeep(versionsToKeep)
    if (mode == TapMode.SOURCE && sourceVersion.isDefined)
      tap.setVersion(sourceVersion.get)
    else if (mode == TapMode.SINK && sinkVersion.isDefined)
      tap.setVersion(sinkVersion.get)
    else
      tap
  }

  val source = getTap(TapMode.SOURCE)
  val sink = getTap(TapMode.SINK)

  def resourceExists(mode: Mode) =
    mode match {
      case Test(buffers) => {
        buffers.get(this) map { !_.isEmpty } getOrElse false
      }
      case HadoopTest(conf, buffers) => {
        buffers.get(this) map { !_.isEmpty } getOrElse false
      }
      case _ => {
        val conf = new JobConf(mode.asInstanceOf[HadoopMode].jobConf)
        source.resourceExists(conf)
      }
    }

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_,_,_] = {
    mode match {
      case Hdfs(_strict, _config) =>
        readOrWrite match {
          case Read  => castHfsTap(source)
          case Write => castHfsTap(sink)
        }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

  // Override this for more control on failure on decode
  protected lazy val checkedInversion: CheckedInversion[(K,V), (Array[Byte],Array[Byte])] =
    new MaxFailuresCheck(maxFailures)(codecBox.get)
  override def transformForRead(pipe: Pipe) = {
    pipe.flatMap((keyField, valField) -> (keyField, valField)) { pair: (Array[Byte],Array[Byte]) =>
      checkedInversion(pair)
    }
  }

  override def transformForWrite(pipe: Pipe) = {
    pipe.mapTo((0,1) -> (keyField, valField)) { pair: (K,V) =>
      codecBox.get.apply(pair)
    }
  }

  override def toString =
    "%s path:%s,sourceVersion:%s,sinkVersion:%s".format(getClass(), path, sourceVersion, sinkVersion)

  override def equals(other: Any) =
    if (other.isInstanceOf[VersionedKeyValSource[_, _]]) {
      val otherSrc = other.asInstanceOf[VersionedKeyValSource[K, V]]
      otherSrc.path == path && otherSrc.sourceVersion == sourceVersion && otherSrc.sinkVersion == sinkVersion
    } else {
      false
    }

  override def hashCode = toString.hashCode
}

object RichPipeEx extends FieldConversions with TupleConversions with java.io.Serializable {
  implicit def pipeToRichPipeEx(pipe: Pipe): RichPipeEx = new RichPipeEx(pipe)
  implicit def typedPipeToRichPipeEx[K: Ordering, V: Monoid](pipe: TypedPipe[(K,V)]) =
    new TypedRichPipeEx(pipe)
}

class TypedRichPipeEx[K: Ordering, V: Monoid](pipe: TypedPipe[(K,V)]) extends java.io.Serializable {
  import Dsl._
  import TDsl._

  // Tap reads existing data from the `sourceVersion` (or latest
  // version) of data specified in `src`, merges the K,V pairs from
  // the pipe in using an implicit `Monoid[V]` and sinks all results
  // into the `sinkVersion` of data (or a new version) specified by
  // `src`.
  def writeIncremental(src: VersionedKeyValSource[K,V], reducers: Int = 1)
  (implicit flowDef: FlowDef, mode: Mode) = {
    val outPipe =
      if (!src.resourceExists(mode))
        pipe
      else {
        val oldPairs = TypedPipe
          .from[(K,V)](src.read, (0,1))
          .map { case (k, v) => (k, v ,0) }

        val newPairs = pipe.map { case (k, v) => (k, v, 1) }

        (oldPairs ++ newPairs)
          .groupBy {  _._1 }
          .withReducers(reducers)
          .sortBy { _._3 }
          .mapValues { _._2 }
          .sum
      }

    outPipe.toPipe((0,1)).write(src)
  }
}

class RichPipeEx(pipe: Pipe) extends java.io.Serializable {
  import Dsl._

  // VersionedKeyValSource always merges with the most recent complete
  // version
  def writeIncremental[K,V](src: VersionedKeyValSource[K,V], fields: Fields, reducers: Int = 1)
  (implicit monoid: Monoid[V],
   flowDef: FlowDef,
   mode: Mode) = {
    def appendToken(pipe: Pipe, token: Int) =
      pipe.mapTo((0,1) -> ('key,'value,'isNew)) { pair: (K,V) => pair :+ token }

    val outPipe =
      if (!src.resourceExists(mode))
        pipe
      else {
        val oldPairs = appendToken(src.read, 0)
        val newPairs = appendToken(pipe, 1)

        (oldPairs ++ newPairs)
          .groupBy('key) { _.reducers(reducers).sortBy('isNew).plus[V]('value) }
          .project(('key,'value))
          .rename(('key, 'value) -> fields)
      }

    outPipe.write(src)
  }
}
