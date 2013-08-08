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

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.elephantbird.mapreduce.io.ThriftWritable
import com.twitter.elephantbird.util.{ ThriftUtils, TypeRef }
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import org.apache.hadoop.io.{ LongWritable, Writable }
import org.apache.thrift.TBase

trait LongThriftTransformer[V <: TBase[_, _]] extends Source {

  def mt: Manifest[V]
  def fields: Fields

  // meant to override fields within WritableSequenceFileScheme.
  val keyType = classOf[LongWritable]
  val valueType = classOf[ThriftWritable[V]].asInstanceOf[Class[Writable]]
  override protected def transformForRead(pipe: Pipe): Pipe = {
    new RichPipe(pipe).mapTo(fields -> fields) { v: (LongWritable, ThriftWritable[V]) =>
      v._2.setConverter(mt.erasure.asInstanceOf[Class[V]])
      (v._1.get, v._2.get)
    }
  }
  override protected def transformForWrite(pipe: Pipe) = {
    new RichPipe(pipe).mapTo(fields -> fields) { v: (Long, V) =>
      val key = new LongWritable(v._1)
      val value = new ThriftWritable(v._2, typeRef)
      (key, value)
    }
  }
  lazy val typeRef = ThriftUtils.getTypeRef(mt.erasure).asInstanceOf[TypeRef[TBase[_, _]]]
}
