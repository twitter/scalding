/*
Copyright 2014 Twitter, Inc.

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

import cascading.flow.{ Flow, FlowProcess }
import cascading.flow.planner.Scope
import cascading.tap.{ SinkMode, Tap }
import cascading.scheme.Scheme
import cascading.tuple._
import cascading.property.ConfigDef

import java.util.{ Set => JSet }

/**
 * This is for when you might want to attach some additional state or behavior
 * onto an existing tap instance (for instance, one returned from createTap
 * in scalding source
 */
abstract class ProxyTap[C, I, O, +T <: Tap[C, I, O]] extends Tap[C, I, O] {
  def proxy: T

  /**
   * This sucks, but I can't see how to make the id the same, and without it
   * this proxy breaks when proxies return this
   */
  @transient private[this] val idF = classOf[Tap[_, _, _]].getDeclaredField("id")
  idF.setAccessible(true)
  idF.set(this, Tap.id(proxy))

  protected def wrapId(id: String): String = id

  override def getScheme: Scheme[C, I, O, _, _] = proxy.getScheme
  override def getTrace: String = proxy.getTrace
  override def flowConfInit(f: Flow[C]) { proxy.flowConfInit(f) }
  override def sourceConfInit(f: FlowProcess[C], conf: C) { proxy.sourceConfInit(f, conf) }
  override def sinkConfInit(f: FlowProcess[C], conf: C) { proxy.sinkConfInit(f, conf) }

  override def getIdentifier: String = wrapId(proxy.getIdentifier)
  override def getSourceFields: Fields = proxy.getSourceFields
  override def getSinkFields: Fields = proxy.getSinkFields

  override def openForRead(fp: FlowProcess[C], in: I): TupleEntryIterator =
    proxy.openForRead(fp, in)
  override def openForRead(fp: FlowProcess[C]): TupleEntryIterator =
    proxy.openForRead(fp)

  override def openForWrite(fp: FlowProcess[C], out: O): TupleEntryCollector =
    proxy.openForWrite(fp, out)
  override def openForWrite(fp: FlowProcess[C]): TupleEntryCollector =
    proxy.openForWrite(fp)

  override def outgoingScopeFor(incoming: JSet[Scope]) =
    proxy.outgoingScopeFor(incoming)

  override def retrieveSourceFields(fp: FlowProcess[C]): Fields =
    proxy.retrieveSourceFields(fp)

  override def presentSourceFields(fp: FlowProcess[C], f: Fields) {
    proxy.presentSourceFields(fp, f)
  }
  override def retrieveSinkFields(fp: FlowProcess[C]): Fields =
    proxy.retrieveSinkFields(fp)

  override def presentSinkFields(fp: FlowProcess[C], f: Fields) {
    proxy.presentSinkFields(fp, f)
  }
  override def resolveIncomingOperationArgumentFields(s: Scope): Fields =
    proxy.resolveIncomingOperationArgumentFields(s)

  override def resolveIncomingOperationPassThroughFields(s: Scope): Fields =
    proxy.resolveIncomingOperationPassThroughFields(s)

  override def getFullIdentifier(fp: FlowProcess[C]): String =
    wrapId(proxy.getFullIdentifier(fp))

  override def getFullIdentifier(conf: C): String =
    wrapId(proxy.getFullIdentifier(conf))

  override def createResource(fp: FlowProcess[C]): Boolean =
    proxy.createResource(fp)

  override def createResource(conf: C): Boolean =
    proxy.createResource(conf)

  override def deleteResource(fp: FlowProcess[C]): Boolean =
    proxy.deleteResource(fp)

  override def deleteResource(conf: C): Boolean =
    proxy.deleteResource(conf)

  override def commitResource(conf: C): Boolean =
    proxy.commitResource(conf)

  override def rollbackResource(conf: C): Boolean =
    proxy.rollbackResource(conf)

  override def resourceExists(fp: FlowProcess[C]): Boolean =
    proxy.resourceExists(fp)

  override def resourceExists(conf: C): Boolean =
    proxy.resourceExists(conf)

  override def getModifiedTime(fp: FlowProcess[C]): Long =
    proxy.getModifiedTime(fp)

  override def getModifiedTime(conf: C): Long =
    proxy.getModifiedTime(conf)

  override def getSinkMode: SinkMode =
    proxy.getSinkMode

  override def isKeep: Boolean = proxy.isKeep
  override def isReplace: Boolean = proxy.isReplace
  override def isUpdate: Boolean = proxy.isUpdate
  override def isSink: Boolean = proxy.isSink
  override def isSource: Boolean = proxy.isSource
  override def isTemporary: Boolean = proxy.isTemporary
  override def getConfigDef: ConfigDef = proxy.getConfigDef
  override def hasConfigDef: Boolean = proxy.hasConfigDef
  override def getStepConfigDef: ConfigDef = proxy.getStepConfigDef
  override def hasStepConfigDef: Boolean = proxy.hasStepConfigDef

  // We are not overriding isEquivalent, equals, hashcode, toString
  // which use getClass, getScheme and getIdentifier
}
