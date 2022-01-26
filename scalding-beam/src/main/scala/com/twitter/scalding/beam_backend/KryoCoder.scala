package com.twitter.scalding.beam_backend

import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.{KryoInstantiator, KryoPool}
import com.twitter.scalding.serialization.JavaStreamEnrichments.{RichInputStream, RichOutputStream}
import com.twitter.scalding.serialization.OrderedSerialization
import java.io.{InputStream, OutputStream}
import org.apache.beam.sdk.coders.{AtomicCoder, Coder}
import scala.language.implicitConversions

final class KryoCoder(kryoInstantiator: KryoInstantiator) extends AtomicCoder[Any] {
  @transient private[this] lazy val kryoPool: KryoPool =
    KryoPool.withByteArrayOutputStream(Runtime.getRuntime.availableProcessors, kryoInstantiator)

  override def encode(value: Any, os: OutputStream): Unit = {
    val bytes = kryoPool.toBytesWithClass(value)
    os.writePosVarInt(bytes.length)
    os.write(bytes)
    os.flush()
  }

  override def decode(is: InputStream): Any = {
    val size = is.readPosVarInt
    val input = new Input(is, size)
    kryoPool.fromBytes(input.readBytes(size))
  }
}

object KryoCoder {
  implicit def castType[T](kryoCoder: KryoCoder): AtomicCoder[T] =
    kryoCoder.asInstanceOf[AtomicCoder[T]]
}

case class OrderedSerializationCoder[T](ordSer: OrderedSerialization[T]) extends AtomicCoder[T] {
  override def encode(value: T, outStream: OutputStream): Unit = ordSer.write(outStream, value)
  override def decode(inStream: InputStream): T = ordSer.read(inStream).get
}

object OrderedSerializationCoder {
  def apply[T](ord: Ordering[T], fallback: Coder[T]): Coder[T] =
    ord match {
      case ordSer: OrderedSerialization[T] @unchecked => OrderedSerializationCoder(ordSer)
      case _                                          => fallback
    }
}

case class TupleCoder[K, V](coderK: Coder[K], coderV: Coder[V]) extends AtomicCoder[(K, V)] {
  override def encode(value: (K, V), outStream: OutputStream): Unit = {
    coderK.encode(value._1, outStream)
    coderV.encode(value._2, outStream)
  }

  override def decode(inStream: InputStream): (K, V) =
    (coderK.decode(inStream), coderV.decode(inStream))
}
