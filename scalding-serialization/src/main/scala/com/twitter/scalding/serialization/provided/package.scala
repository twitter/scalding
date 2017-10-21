package com.twitter.scalding.serialization

import java.nio.ByteBuffer

import com.twitter.scalding.serialization.provided.{ OrderedSerializationByteBuffer }

package object provided {
  implicit val byteBufferOrderedSerialization: Exported[OrderedSerialization[ByteBuffer]] = Exported(OrderedSerializationByteBuffer)
  implicit def eitherOrderedOrderedSerialization[L: OrderedSerialization, R: OrderedSerialization]: Exported[OrderedSerialization[Either[L, R]]] = Exported(EitherOrderedSerialization())
}
