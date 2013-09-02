/*
Copyright 2013 Twitter, Inc.

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

import java.io.{ByteArrayOutputStream, Externalizable, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.Kryo

object Externalizer {
  def apply[T](t: T): Externalizer[T] = {
    val x = new Externalizer[T]
    x.set(t)
    x
  }
}

/** This is a class that tries first to do Kryo serialization,
 * and then falls back to Java serialization if that does not
 * work, which of course may fail if the contained item is not
 * Java serializable
 */
class Externalizer[T] extends Externalizable {
  private var item: Option[T] = None

  def getOption: Option[T] = item
  def get: T = item.get // This should never be None when get is called

  /** Unfortunately, Java serialization requires mutable objects if
   * you are going to control how the serialization is done.
   * Use the companion object to creat new instances of this
   */
  def set(it: T): Unit = {
    assert(item.isEmpty, "Tried to call .set on an already constructed Externalizer")
    item = Some(it)
  }

  private val KRYO = 0
  private val JAVA = 1

  /** Use the same mechanism as scalding to generate a Kryo instance */
  protected def kryo: Kryo = {
    val kh = new KryoHadoop
    val k = kh.newKryo
    kh.decorateKryo(k)
    // need support for graphs with loops:
    k.setReferences(true)
    k
  }

  private def safeToBytes: Option[Array[Byte]] = {
    try {
      val outS = new ByteArrayOutputStream
      val kout = new Output(outS)
      val k = kryo
      k.writeClassAndObject(kout, item)
      kout.flush
      val bytes = outS.toByteArray
      // Make sure we can read without throwing
      k.readClassAndObject(new Input(bytes))
      Some(bytes)
    }
    catch {
      case t: Throwable =>
        Option(System.getenv.get("SCALDING_EXTERNALIZER_DEBUG"))
          .filter(_.toBoolean)
          .foreach { _ => t.printStackTrace }
        None
    }
  }
  private def fromBytes(b: Array[Byte]): Option[T] = {
    val k = kryo
    k.readClassAndObject(new Input(b)).asInstanceOf[Option[T]]
  }

  def readExternal(in: ObjectInput) {
    in.read match {
      case KRYO =>
        val sz = in.readInt
        val buf = new Array[Byte](sz)
        in.readFully(buf)
        item = fromBytes(buf)
      case JAVA =>
        item = in.readObject.asInstanceOf[Option[T]]
    }
  }
  def writeExternal(out: ObjectOutput) {
    safeToBytes match {
      case Some(bytes) =>
        out.write(KRYO)
        out.writeInt(bytes.size)
        out.write(bytes)
      case None =>
        out.write(JAVA)
        out.writeObject(item)
    }
  }
}
