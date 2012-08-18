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
package com.twitter.scalding.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import com.twitter.algebird.{AveragedValue, DecayedValue, HLLInstance}

class AveragedValueSerializer extends KSerializer[AveragedValue] {
  setImmutable(true)
  def write(kser: Kryo, out : Output, s : AveragedValue) {
    out.writeLong(s.count, true)
    out.writeDouble(s.value)
  }
  def read(kser : Kryo, in : Input, cls : Class[AveragedValue]) : AveragedValue =
    AveragedValue(in.readLong(true), in.readDouble)
}

class DecayedValueSerializer extends KSerializer[DecayedValue] {
  setImmutable(true)
  def write(kser: Kryo, out : Output, s : DecayedValue) {
    out.writeDouble(s.value)
    out.writeDouble(s.scaledTime)
  }
  def read(kser : Kryo, in : Input, cls : Class[DecayedValue]) : DecayedValue =
    DecayedValue(in.readDouble, in.readDouble)
}

class HLLSerializer extends KSerializer[HLLInstance] {
  setImmutable(true)
  def write(kser: Kryo, out : Output, s : HLLInstance) {
    val vals = s.v
    out.writeInt(vals.size, true)
    vals.foreach { out.writeInt(_, true) }
  }
  def read(kser : Kryo, in : Input, cls : Class[HLLInstance]) : HLLInstance = {
    val array = new Array[Int](in.readInt(true))
    (0 until array.length) foreach { array(_) = in.readInt(true) }
    HLLInstance(array.toIndexedSeq)
  }
}
