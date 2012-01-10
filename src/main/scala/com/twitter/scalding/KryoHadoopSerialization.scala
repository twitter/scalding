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
package com.twitter.scalding

import java.io.InputStream
import java.io.OutputStream
import java.io.Serializable
import java.nio.ByteBuffer

import org.apache.hadoop.io.serializer.{Serialization, Deserializer, Serializer, WritableSerialization}

//This version of Kryo has support for objects without default constructors, which
//are not uncommon in scala
import de.javakaffee.kryoserializers.{KryoReflectionFactorySupport=>Kryo}
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import cascading.tuple.hadoop.BufferedInputStream
import cascading.tuple.hadoop.TupleSerialization

import scala.annotation.tailrec

/**
* Use Kryo to serialize any object.  Thrift and Protobuf
* are strictly more efficient, but this allows any object to
* be used.  Recommended for objects sent between mappers and reducers
* not for final output serialization.
*
* Modeled on:
* https://github.com/mesos/spark/blob/master/core/src/main/scala/spark/KryoSerializer.scala
*
* @author Oscar Boykin
*/

@serializable
object KryoHadoopSerialization {
  def readSize(instream : InputStream) = {
    @tailrec
    def sizeR(bytes : Int) : Int = {
      // 1 -> 256 - 1, 2 -> 256**2 - 1, 4-> 256**4 - 1
      val maxValue = (1L << (8*(bytes))) - 1
      //This is just a big-endian read:
      val thisValue = (1 to bytes).foldLeft(0) {
        (acc, _) => (acc << 8) + instream.read
      }
      if (thisValue < maxValue) {
        thisValue
      }
      else {
        //Try again:
        sizeR(bytes * 2)
      }
    }
    sizeR(1)
  }

  /** Returns the total number of bytes written into the
  * the stream
  */
  def writeSize(os : OutputStream, sz : Int) = {
    @tailrec
    def writeR(written : Int, bytes : Int) : Int = {
      val maxValue = (1L << (8*(bytes))) - 1
      if( sz < maxValue ) {
        //Do a big endian write:
        (0 until bytes).reverse.foreach { shift =>
          os.write(sz >> (8*shift))
        }
        written + bytes
      }
      else {
        //Signal it was too big:
        (0 until bytes).foreach { (_) => os.write(0xff) }
        //Try with twice as many bytes:
        writeR(written + bytes, 2*bytes)
      }
    }
    writeR(0,1)
  }
}

// Singletons are easy, you just return the singleton and don't read:
// It's important you actually do this, or Kryo will generate Nil != Nil, or None != None
class SingletonSerializer(obj: AnyRef) extends KSerializer {
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {}
  override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = obj.asInstanceOf[T]
}

// Lists cause stack overflows for Kryo because they are cons cells.
class ListSerializer(kser : Kryo) extends KSerializer {
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {
    //Write the size:
    val list = obj.asInstanceOf[List[AnyRef]]
    kser.writeObjectData(buf, new java.lang.Integer(list.size))
    /*
     * An excellent question arrises at this point:
     * How do we deal with List[List[T]]?
     * Since by the time this method is called, the ListSerializer has
     * already been registered, this iterative method will be used on
     * each element, and we should be safe.
     * The only risk is List[List[List[List[.....
     * But anyone who tries that gets what they deserve
     */
    list.foreach { t => kser.writeClassAndObject(buf, t) }
  }

  override def readObjectData[T](buf: ByteBuffer, cls: Class[T]) : T = {
    val size = kser.readObjectData(buf, classOf[java.lang.Integer]).intValue
    //Produce the reversed list:
    if (size == 0) {
      /*
       * this is only here at compile time.  The type T is erased, but the
       * compiler verifies that we are intending to return a type T here.
       */
      Nil.asInstanceOf[T]
    }
    else {
      (0 until size).foldLeft(List[AnyRef]()) { (l, i) =>
        val iT = kser.readClassAndObject(buf)
        iT :: l
      }.reverse.asInstanceOf[T]
    }
  }
}

class KryoHadoopSerialization[T] extends Serialization[T] with Serializable {

  val INIT_SIZE = 4 * 1024 //4kB to start, double if needed

  //TODO a more robust way to handle being low priority should go here.  For now, it looks like
  //the only serializer added after we add user io.serializations is TupleSerialization, so we are
  //handling it below.  A better scheme is to look at the JobConf, and instantiate all the named
  //serializers. For now, we are waiting until this actually poses itself as a problem
  val highPrioritySerializations = List(new WritableSerialization, new TupleSerialization)

  def newKryo(klass : Class[_]) = {
    val newK = new Kryo
    newK.setRegistrationOptional(true)
    /* Here is some jank for your reading pleasure:
     * Kryo looks up the serializer in a hashtable of java.lang.Class objects.
     * What if X is a subclass of Y and my serialization is co-variant?
     * TOO BAD!  Scala uses class scala.collection.immutable.List if you get classOf[List[_]]
     * but class scala.collection.immutable.$colon$colon for most concrete instances
     * This deals with the case of containers holding lists, even if klass is not directly one.
     */
    val listSer = new ListSerializer(newK)
    newK.register(List(1).getClass, listSer)
    //Make sure to register the Nil singleton, different from List(1).getClass
    newK.register(Nil.getClass, listSer)
    //Deal with the case that we directly have a list:
    Option(klass).foreach { cls =>
      if (classOf[List[_]].isAssignableFrom(cls)) {
        newK.register(cls, listSer)
      } else {
        try {
          newK.register(cls)
        } catch {
          case iae : java.lang.IllegalArgumentException => {
            // This is because we tried to register a type twice.
            // That is okay, nothing is really wrong, this can
            // happen if you are lazy and register types like java.lang.Integer
            // which Hadoop is not going to use Kryo for anyway.
          }
        }
      }
    }
    //Add commonly used types:
    registeredTypes.foreach { cls => newK.register(cls) }
    // Register some commonly used Scala singleton objects. Because these
    // are singletons, we must return the exact same local object when we
    // deserialize rather than returning a clone as FieldSerializer would.
    // TODO there may be a smarter way to detect scala object (singletons)
    // and do this automatically
    //This includes Nil:
    singletons.foreach { inst => newK.register( inst.getClass, new SingletonSerializer(inst) ) }
    newK
  }

  //Put any singleton objects that should be serialized here
  def singletons : Iterable[AnyRef] = {
    //Note: Nil is a singleton, but handled by the ListSerializer correctly
    List(None)
  }

  // Types to pre-register.
  // TODO: this was cargo-culted from spark. We should actually measure to see the best
  // choices for the common use cases. Since hadoop tells us the class we are deserializing
  // the benefit of this is much less than spark
  def registeredTypes : List[Class[_]] = {
    List(
      // Arrays
      Array(1), Array(1.0), Array(1.0f), Array(1L), Array(""), Array(("", "")),
      Array(new java.lang.Object), Array(1.toByte), Array(true), Array('c'),
      // Specialized Tuple2s
      ("", ""), (1, 1), (1.0, 1.0), (1L, 1L),
      (1, 1.0), (1.0, 1), (1L, 1.0), (1.0, 1L), (1, 1L), (1L, 1),
      // Options and Either
      Some(1), Left(1), Right(1),
      // Higher-dimensional tuples
      (1, 1, 1), (1, 1, 1, 1), (1, 1, 1, 1, 1)
    ).map { _.getClass }
  }

  //We should handle everything the higher priority doesn't
  override def accept(klass : Class[_]) = highPrioritySerializations.forall { !_.accept(klass) }
  override def getDeserializer(klass : Class[T]) = new KryoDeserializer[T](klass)
  override def getSerializer(klass : Class[T]) = new KryoSerializer[T](klass)

  class KryoSerializer[T](klass : Class[T]) extends Serializer[T] with Serializable {
    var out : Option[OutputStream] = None
    var kryo : Option[Kryo] = None
    var buf = ByteBuffer.wrap(new Array[Byte](INIT_SIZE))
    val SIZE_MULT = 1.5 //From Java default does 3*size/2 + 1
    /**
    * this keeps trying to write, increasing the buf each time until the
    * allocation fails.
    */
    @tailrec
    private def allocatingWrite(obj : Any) : Unit = {
      val k = kryo.get
      buf.rewind
      val start = buf.position
      val finished = try {
        k.writeObjectData(buf, obj)
        val end = buf.position
        //write the size:
        val size = end - start
        //Now put into the OutputStream:
        val os = out.get
        KryoHadoopSerialization.writeSize(os, size)
        os.write(buf.array, start, size)
        true
      }
      catch {
        case ex: com.esotericsoftware.kryo.SerializationException => {
          //Take a step up if we need to realloc:
          buf = ByteBuffer.wrap(new Array[Byte]((SIZE_MULT * buf.capacity).toInt + 1))
          false
        }
      }
      if (!finished) {
        allocatingWrite(obj)
      }
    }

    override def open(output : OutputStream) {
      out = Some(output)
      //We have to reset the kryo serializer each time because
      //the order in which we see classes defines the codes given to them.
      val nk = newKryo(klass)
      kryo = Some(nk)
    }

    override def serialize(obj : T) {
      allocatingWrite(obj)
    }

    override def close {
      out.map { _.close }
      out = None
      kryo = None
    }

  }

  class KryoDeserializer[T](klass : Class[T]) extends Deserializer[T] with Serializable {
    var in : Option[InputStream] = None
    var kryo : Option[Kryo] = None
    var buf = ByteBuffer.wrap(new Array[Byte](INIT_SIZE))

    private def ensureAlloc(sz : Int) {
      if (buf.capacity < sz) {
        buf = ByteBuffer.wrap(new Array[Byte](sz))
      }
    }

    override def open(input : InputStream) {
      in = Some(input)
      //We have to reset the kryo serializer each time because
      //the order in which we see classes defines the codes given to them.
      val nk = newKryo(klass)
      kryo = Some(nk)
    }

    override def deserialize(ignored : T) : T = {
      val k = kryo.get
      //read into our ByteBuffer:
      val size = KryoHadoopSerialization.readSize(in.get)
      ensureAlloc(size)
      buf.rewind
      in.get.read(buf.array, buf.position, size)
      k.readObjectData[T](buf, klass)
    }

    override def close {
      in.map { _.close }
      in = None
      kryo = None
    }
  }
}
