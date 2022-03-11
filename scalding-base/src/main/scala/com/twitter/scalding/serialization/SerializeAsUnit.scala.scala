package com.twitter.scalding.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

// We use this for TypedPipe subclasses which should never be needed when we run
class SerializeAsUnit[T >: Null] extends KSerializer[T] {
  override def write(kryo: Kryo, output: Output, t: T): Unit = ()
  override def read(kryo: Kryo, input: Input, t: Class[T]): T = null
}
