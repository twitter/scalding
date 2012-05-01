package com.twitter.scalding

import org.specs._

import java.io.{ByteArrayOutputStream=>BOS}
import java.io.{ByteArrayInputStream=>BIS}

import scala.collection.immutable.ListMap

/*
* This is just a test case for Kryo to deal with. It should
* be outside KryoTest, otherwise the enclosing class, KryoTest
* will also need to be serialized
*/
case class TestCaseClassForSerialization(x : String, y : Int)

case class TestValMap(val map : Map[String,Double])

class KryoTest extends Specification {

  noDetailedDiffs() //Fixes issue for scala 2.9

  def serObj[T <: AnyRef](in : T) = {
    val khs = new KryoHadoopSerialization
    khs.accept(in.getClass)
    val ks = khs.getSerializer(in.getClass.asInstanceOf[Class[AnyRef]])
    val out = new BOS
    ks.open(out)
    ks.serialize(in)
    ks.close
    out.toByteArray
  }

  def deserObj[T <: AnyRef](cls : Class[_], input : Array[Byte]) : T = {
    val khs = new KryoHadoopSerialization
    khs.accept(cls)
    val ks = khs.getDeserializer(cls.asInstanceOf[Class[AnyRef]])
    val in = new BIS(input)
    ks.open(in)
    val fakeInputHadoopNeeds = null
    val res = ks.deserialize(fakeInputHadoopNeeds.asInstanceOf[T])
    ks.close
    res.asInstanceOf[T]
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
                      ListMap("good" -> 0.5, "bad" -> -1.0),
                      TestCaseClassForSerialization("case classes are: ", 10),
                      TestValMap(Map("you" -> 1.0, "every" -> 2.0, "body" -> 3.0, "a" -> 1.0,
                        "b" -> 2.0, "c" -> 3.0, "d" -> 4.0)),
                      Vector(1,2,3,4,5),
                      TestValMap(null),
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
    "handle Date, RichDate and DateRange" in {
      import DateOps._
      implicit val tz = PACIFIC
      val myDate : RichDate = "1999-12-30T14"
      val simpleDate : java.util.Date = myDate.value
      val myDateRange = DateRange("2012-01-02", "2012-06-09")
      singleRT(myDate) must be_==(myDate)
      singleRT(simpleDate) must be_==(simpleDate)
      singleRT(myDateRange) must be_==(myDateRange)
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
