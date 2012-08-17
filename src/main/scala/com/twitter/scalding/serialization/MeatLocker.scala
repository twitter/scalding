package com.twitter.scalding.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer

import org.apache.commons.codec.binary.Base64
import org.objenesis.strategy.StdInstantiatorStrategy

trait KryoSerializer {
  def getKryo : Kryo = {
    val k = new Kryo {
      lazy val objSer = new ObjectSerializer[AnyRef]

      override def getDefaultSerializer(klass : Class[_]) : KSerializer[_] = {
        if(isSingleton(klass))
          objSer
        else
          super.getDefaultSerializer(klass)
      }

      def isSingleton(klass : Class[_]) : Boolean = {
        classOf[scala.ScalaObject].isAssignableFrom(klass) &&
          klass.getName.last == '$'
      }
    }

    k.setRegistrationRequired(false)
    k.setInstantiatorStrategy(new StdInstantiatorStrategy)
    k
  }

  def serialize(ag : AnyRef) : Array[Byte] = {
    val output = new Output(1 << 12, 1 << 30)
    getKryo.writeClassAndObject(output, ag)
    output.toBytes
  }

  def deserialize[T](bytes : Array[Byte]) : T = {
    getKryo.readClassAndObject(new Input(bytes))
      .asInstanceOf[T]
  }

  def serializeBase64(ag: AnyRef): String =
    Base64.encodeBase64String(serialize(ag))

  def deserializeBase64[T](string: String): T =
    deserialize[T](Base64.decodeBase64(string))
}

class MeatLocker[T](@transient t : T) extends KryoSerializer with java.io.Serializable {
  protected val tBytes = serialize(t.asInstanceOf[AnyRef])
  lazy val get : T = deserialize[T](tBytes)
}
