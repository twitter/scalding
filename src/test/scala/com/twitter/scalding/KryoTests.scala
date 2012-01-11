package com.twitter.scalding

import org.specs._

import java.io.{ByteArrayOutputStream=>BOS}
import java.io.{ByteArrayInputStream=>BIS}

/*
* This is just a test case for Kryo to deal with. It should
* be outside KryoTest, otherwise the enclosing class, KryoTest
* will also need to be serialized
*/
case class TestCaseClassForSerialization(x : String, y : Int)

class KryoTest extends Specification {

  noDetailedDiffs() //Fixes issue for scala 2.9

  def roundTrip(in : List[Int]) = {
    val outs = new BOS
    in.foreach { i => KryoHadoopSerialization.writeSize(outs, i) }
    val ins = new BIS(outs.toByteArray)
    //Read each back in and return it:
    in.map { (_) => KryoHadoopSerialization.readSize(ins) }
  }
  def rtEnds(upper : Int) {
    //Test the end points:
    val ends = List(0,1,2,upper - 2,upper - 1)
    roundTrip(ends) must be_==(ends)
  }
  def rtRands(upper : Int) {
    val r = new java.util.Random
    val rands = (1 to 20).map { (_) => r.nextInt(upper) }.toList
    roundTrip(rands) must be_==(rands)
  }

  "KryoHadoopSerialization" should {
    "read and write sizes near end points" in {
      rtEnds(0xff)
      rtEnds(0xffff)
      rtEnds(Int.MaxValue)
    }
    "read and write sizes of random values" in {
      rtRands(0xff)
      rtRands(0xffff)
      rtRands(Int.MaxValue)
    }
    /*
    Something about this test triggers a compiler bug
    "return serializers/deserializers/comparators" in {
      val khs = new KryoHadoopSerialization
      khs.getSerializer(null) must notBeNull
      khs.getDeserializer(null) must notBeNull
      khs.getComparator(null) must notBeNull
    }*/
  }

  def serObj[T <: AnyRef](in : T) = {
    val khs = new KryoHadoopSerialization[T]
    val ks = khs.getSerializer(in.getClass.asInstanceOf[Class[T]])
    val out = new BOS
    ks.open(out)
    ks.serialize(in)
    ks.close
    out.toByteArray
  }

  def deserObj[T <: AnyRef](cls : Class[_], input : Array[Byte]) : T = {
    val khs = new KryoHadoopSerialization[T]
    val ks = khs.getDeserializer(cls.asInstanceOf[Class[T]])
    val in = new BIS(input)
    ks.open(in)
    val fakeInputHadoopNeeds = null
    val res = ks.deserialize(fakeInputHadoopNeeds.asInstanceOf[T])
    ks.close
    res
  }
  def singleRT[T <: AnyRef](in : T) : T = {
    deserObj[T](in.getClass, serObj(in))
  }

  //These are analogous to how Hadoop will serialize
  def serialize(ins : List[AnyRef]) = {
    ins.map { v => (v.getClass, serObj(v)) }
  }
  def deserialize(input : List[(Class[_], Array[Byte])]) = {
    input.map { tup => deserObj[AnyRef](tup._1, tup._2) }
  }
  def serializationRT(ins : List[AnyRef]) = deserialize(serialize(ins))


  "KryoSerializers and KryoDeserializers" should {
    "round trip any non-array object" in {
      val test = List(1,2,"hey",(1,2),
                      ("hey","you"),Map(1->2,4->5),0 to 100,
                      (0 to 42).toList, Seq(1,100,1000),
                      Map("good" -> 0.5, "bad" -> -1.0),
                      TestCaseClassForSerialization("case classes are: ", 10),
                      Some("junk"))
        .asInstanceOf[List[AnyRef]]
      serializationRT(test) must be_==(test)
    }
    "handle arrays" in {
      def arrayRT[T](arr : Array[T]) {
        serializationRT(List(arr))(0)
          .asInstanceOf[Array[T]].toList must be_==(arr.toList)
      }
      arrayRT(Array(0))
      arrayRT(Array(0.1))
      arrayRT(Array("hey"))
      arrayRT(Array((0,1)))
      arrayRT(Array(None, Nil, None, Nil))
    }
    "handle scala singletons" in {
      val test = List(Nil, None)
      //Serialize each:
      serializationRT(test) must be_==(test)
      //Together in a list:
      singleRT(test) must be_==(test)
    }
    "Serialize a giant list" in {
      val bigList = (1 to 100000).toList
      val list2 = deserObj[List[Int]](bigList.getClass, serObj(bigList))
      //Specs, it turns out, also doesn't deal with giant lists well:
      list2.zip(bigList).foreach { tup =>
        tup._1 must be_==(tup._2)
      }
    }
  }
}
