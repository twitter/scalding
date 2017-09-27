package com.twitter.scalding.serialization

import java.nio.ByteBuffer

import com.twitter.scalding.serialization.provided.{ OrderedSerializationByteBuffer }

package object provided {
  implicit def byteBufferOrderedSerialization: Exported[OrderedSerialization[ByteBuffer]] = Exported(OrderedSerializationByteBuffer)
}
