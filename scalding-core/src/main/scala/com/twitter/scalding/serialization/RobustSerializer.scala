package com.twitter.scalding.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{ Serializer => KSerializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.serializers.{ JavaSerializer, FieldSerializer }
import scala.util.Try
import java.io.{
  ByteArrayOutputStream,
  ByteArrayInputStream,
  ObjectInputStream,
  ObjectOutputStream
}

/**
 * This is a default serializer that tries first to successfully
 * use java serialization, then fall back to kryo. Since
 * this is the default, when it falls back to kyro field
 * by field we will recursively try the same
 */
class RobustSerializer[A](k: Kryo, cls: Class[A]) extends KSerializer[A] {
  private[this] val javaSer: KSerializer[A] = (new JavaSerializer).asInstanceOf[KSerializer[A]]
  private[this] val fieldSer = new FieldSerializer[A](k, cls)

  override def write(kryo: Kryo, output: Output, t: A): Unit = {
    val s = Try {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(t)
      val bytes = baos.toByteArray
      val testInput = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(testInput)
      ois.readObject // this may throw
    }
    if (s.isSuccess) {
      println(s"works! $t")
      // if the above did not throw, use java
      output.write(0)
      javaSer.write(kryo, output, t)
    } else {
      println(s"does not $t")
      output.write(1)
      fieldSer.write(kryo, output, t)
    }
  }

  override def read(kryo: Kryo, input: Input, t: Class[A]): A =
    input.read() match {
      case 0 => javaSer.read(kryo, input, t).asInstanceOf[A]
      case 1 => fieldSer.read(kryo, input, t)
      case x => sys.error(s"unexpected serialization type: $x for class $t")
    }
}
