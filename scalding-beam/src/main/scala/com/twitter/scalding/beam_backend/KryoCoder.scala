package com.twitter.scalding.beam_backend

import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.{ KryoInstantiator, KryoPool }
import com.twitter.scalding.serialization.JavaStreamEnrichments.{ RichInputStream, RichOutputStream }
import java.io.{ InputStream, OutputStream }
import org.apache.beam.sdk.coders.AtomicCoder
import scala.language.implicitConversions

final class KryoCoder(kryoInstantiator: KryoInstantiator) extends AtomicCoder[Any] {
  @transient private[this] lazy val kryoPool: KryoPool = KryoPool.withByteArrayOutputStream(Runtime
    .getRuntime
    .availableProcessors, kryoInstantiator)
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
