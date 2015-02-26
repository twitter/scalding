/*
Copyright 2015 Twitter, Inc.

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

import java.util.concurrent.atomic.AtomicReference
import java.io.{ InputStream, OutputStream }

import com.esotericsoftware.kryo.{ Serializer => KSerializer, DefaultSerializer, Kryo }
import com.esotericsoftware.kryo.io.{ Input, Output }

/**
 * This interface is a way of wrapping a value in a marker class
 * whose class identity is used to control which serialization we
 * use. This is an internal implementation detail about how we
 * interact with cascading and hadoop. Users should never care.
 */
trait Boxed[+K] {
  def get: K
}

class BoxedDefaultSerialization extends KSerializer[Boxed[_]] {
  override def write(kryo: Kryo, output: Output, t: Boxed[_]) {
    sys.error(s"Kryo should never be used to serialize a boxed instance: $t")
  }
  override def read(kryo: Kryo, input: Input, t: Class[Boxed[_]]): Boxed[_] =
    sys.error("Kryo should never be used to serialize a boxed instance, class: $t")
}

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed0[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed1[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed2[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed3[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed4[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed5[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed6[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed7[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed8[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed9[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed10[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed11[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed12[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed13[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed14[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed15[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed16[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed17[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed18[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed19[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed20[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed21[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed22[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed23[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed24[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed25[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed26[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed27[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed28[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed29[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed30[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed31[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed32[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed33[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed34[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed35[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed36[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed37[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed38[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed39[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed40[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed41[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed42[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed43[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed44[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed45[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed46[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed47[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed48[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed49[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed50[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed51[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed52[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed53[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed54[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed55[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed56[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed57[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed58[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed59[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed60[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed61[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed62[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed63[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed64[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed65[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed66[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed67[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed68[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed69[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed70[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed71[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed72[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed73[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed74[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed75[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed76[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed77[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed78[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed79[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed80[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed81[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed82[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed83[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed84[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed85[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed86[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed87[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed88[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed89[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed90[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed91[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed92[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed93[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed94[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed95[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed96[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed97[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed98[K](override val get: K) extends Boxed[K]

@DefaultSerializer(classOf[BoxedDefaultSerialization])
class Boxed99[K](override val get: K) extends Boxed[K]

case class BoxedOrderedSerialization[K](box: K => Boxed[K],
  ord: OrderedSerialization[K]) extends OrderedSerialization[Boxed[K]] {

  override def compare(a: Boxed[K], b: Boxed[K]) = ord.compare(a.get, b.get)
  override def hash(k: Boxed[K]) = ord.hash(k.get)
  override def compareBinary(a: InputStream, b: InputStream) = ord.compareBinary(a, b)
  override def read(from: InputStream) = ord.read(from).map(box)
  override def write(into: OutputStream, bk: Boxed[K]) = ord.write(into, bk.get)
}

object Boxed {
  private[this] val allBoxes = List(
    ({ t: Any => new Boxed0(t) }, classOf[Boxed0[Any]]),
    ({ t: Any => new Boxed1(t) }, classOf[Boxed1[Any]]),
    ({ t: Any => new Boxed2(t) }, classOf[Boxed2[Any]]),
    ({ t: Any => new Boxed3(t) }, classOf[Boxed3[Any]]),
    ({ t: Any => new Boxed4(t) }, classOf[Boxed4[Any]]),
    ({ t: Any => new Boxed5(t) }, classOf[Boxed5[Any]]),
    ({ t: Any => new Boxed6(t) }, classOf[Boxed6[Any]]),
    ({ t: Any => new Boxed7(t) }, classOf[Boxed7[Any]]),
    ({ t: Any => new Boxed8(t) }, classOf[Boxed8[Any]]),
    ({ t: Any => new Boxed9(t) }, classOf[Boxed9[Any]]),
    ({ t: Any => new Boxed10(t) }, classOf[Boxed10[Any]]),
    ({ t: Any => new Boxed11(t) }, classOf[Boxed11[Any]]),
    ({ t: Any => new Boxed12(t) }, classOf[Boxed12[Any]]),
    ({ t: Any => new Boxed13(t) }, classOf[Boxed13[Any]]),
    ({ t: Any => new Boxed14(t) }, classOf[Boxed14[Any]]),
    ({ t: Any => new Boxed15(t) }, classOf[Boxed15[Any]]),
    ({ t: Any => new Boxed16(t) }, classOf[Boxed16[Any]]),
    ({ t: Any => new Boxed17(t) }, classOf[Boxed17[Any]]),
    ({ t: Any => new Boxed18(t) }, classOf[Boxed18[Any]]),
    ({ t: Any => new Boxed19(t) }, classOf[Boxed19[Any]]),
    ({ t: Any => new Boxed20(t) }, classOf[Boxed20[Any]]),
    ({ t: Any => new Boxed21(t) }, classOf[Boxed21[Any]]),
    ({ t: Any => new Boxed22(t) }, classOf[Boxed22[Any]]),
    ({ t: Any => new Boxed23(t) }, classOf[Boxed23[Any]]),
    ({ t: Any => new Boxed24(t) }, classOf[Boxed24[Any]]),
    ({ t: Any => new Boxed25(t) }, classOf[Boxed25[Any]]),
    ({ t: Any => new Boxed26(t) }, classOf[Boxed26[Any]]),
    ({ t: Any => new Boxed27(t) }, classOf[Boxed27[Any]]),
    ({ t: Any => new Boxed28(t) }, classOf[Boxed28[Any]]),
    ({ t: Any => new Boxed29(t) }, classOf[Boxed29[Any]]),
    ({ t: Any => new Boxed30(t) }, classOf[Boxed30[Any]]),
    ({ t: Any => new Boxed31(t) }, classOf[Boxed31[Any]]),
    ({ t: Any => new Boxed32(t) }, classOf[Boxed32[Any]]),
    ({ t: Any => new Boxed33(t) }, classOf[Boxed33[Any]]),
    ({ t: Any => new Boxed34(t) }, classOf[Boxed34[Any]]),
    ({ t: Any => new Boxed35(t) }, classOf[Boxed35[Any]]),
    ({ t: Any => new Boxed36(t) }, classOf[Boxed36[Any]]),
    ({ t: Any => new Boxed37(t) }, classOf[Boxed37[Any]]),
    ({ t: Any => new Boxed38(t) }, classOf[Boxed38[Any]]),
    ({ t: Any => new Boxed39(t) }, classOf[Boxed39[Any]]),
    ({ t: Any => new Boxed40(t) }, classOf[Boxed40[Any]]),
    ({ t: Any => new Boxed41(t) }, classOf[Boxed41[Any]]),
    ({ t: Any => new Boxed42(t) }, classOf[Boxed42[Any]]),
    ({ t: Any => new Boxed43(t) }, classOf[Boxed43[Any]]),
    ({ t: Any => new Boxed44(t) }, classOf[Boxed44[Any]]),
    ({ t: Any => new Boxed45(t) }, classOf[Boxed45[Any]]),
    ({ t: Any => new Boxed46(t) }, classOf[Boxed46[Any]]),
    ({ t: Any => new Boxed47(t) }, classOf[Boxed47[Any]]),
    ({ t: Any => new Boxed48(t) }, classOf[Boxed48[Any]]),
    ({ t: Any => new Boxed49(t) }, classOf[Boxed49[Any]]),
    ({ t: Any => new Boxed50(t) }, classOf[Boxed50[Any]]),
    ({ t: Any => new Boxed51(t) }, classOf[Boxed51[Any]]),
    ({ t: Any => new Boxed52(t) }, classOf[Boxed52[Any]]),
    ({ t: Any => new Boxed53(t) }, classOf[Boxed53[Any]]),
    ({ t: Any => new Boxed54(t) }, classOf[Boxed54[Any]]),
    ({ t: Any => new Boxed55(t) }, classOf[Boxed55[Any]]),
    ({ t: Any => new Boxed56(t) }, classOf[Boxed56[Any]]),
    ({ t: Any => new Boxed57(t) }, classOf[Boxed57[Any]]),
    ({ t: Any => new Boxed58(t) }, classOf[Boxed58[Any]]),
    ({ t: Any => new Boxed59(t) }, classOf[Boxed59[Any]]),
    ({ t: Any => new Boxed60(t) }, classOf[Boxed60[Any]]),
    ({ t: Any => new Boxed61(t) }, classOf[Boxed61[Any]]),
    ({ t: Any => new Boxed62(t) }, classOf[Boxed62[Any]]),
    ({ t: Any => new Boxed63(t) }, classOf[Boxed63[Any]]),
    ({ t: Any => new Boxed64(t) }, classOf[Boxed64[Any]]),
    ({ t: Any => new Boxed65(t) }, classOf[Boxed65[Any]]),
    ({ t: Any => new Boxed66(t) }, classOf[Boxed66[Any]]),
    ({ t: Any => new Boxed67(t) }, classOf[Boxed67[Any]]),
    ({ t: Any => new Boxed68(t) }, classOf[Boxed68[Any]]),
    ({ t: Any => new Boxed69(t) }, classOf[Boxed69[Any]]),
    ({ t: Any => new Boxed70(t) }, classOf[Boxed70[Any]]),
    ({ t: Any => new Boxed71(t) }, classOf[Boxed71[Any]]),
    ({ t: Any => new Boxed72(t) }, classOf[Boxed72[Any]]),
    ({ t: Any => new Boxed73(t) }, classOf[Boxed73[Any]]),
    ({ t: Any => new Boxed74(t) }, classOf[Boxed74[Any]]),
    ({ t: Any => new Boxed75(t) }, classOf[Boxed75[Any]]),
    ({ t: Any => new Boxed76(t) }, classOf[Boxed76[Any]]),
    ({ t: Any => new Boxed77(t) }, classOf[Boxed77[Any]]),
    ({ t: Any => new Boxed78(t) }, classOf[Boxed78[Any]]),
    ({ t: Any => new Boxed79(t) }, classOf[Boxed79[Any]]),
    ({ t: Any => new Boxed80(t) }, classOf[Boxed80[Any]]),
    ({ t: Any => new Boxed81(t) }, classOf[Boxed81[Any]]),
    ({ t: Any => new Boxed82(t) }, classOf[Boxed82[Any]]),
    ({ t: Any => new Boxed83(t) }, classOf[Boxed83[Any]]),
    ({ t: Any => new Boxed84(t) }, classOf[Boxed84[Any]]),
    ({ t: Any => new Boxed85(t) }, classOf[Boxed85[Any]]),
    ({ t: Any => new Boxed86(t) }, classOf[Boxed86[Any]]),
    ({ t: Any => new Boxed87(t) }, classOf[Boxed87[Any]]),
    ({ t: Any => new Boxed88(t) }, classOf[Boxed88[Any]]),
    ({ t: Any => new Boxed89(t) }, classOf[Boxed89[Any]]),
    ({ t: Any => new Boxed90(t) }, classOf[Boxed90[Any]]),
    ({ t: Any => new Boxed91(t) }, classOf[Boxed91[Any]]),
    ({ t: Any => new Boxed92(t) }, classOf[Boxed92[Any]]),
    ({ t: Any => new Boxed93(t) }, classOf[Boxed93[Any]]),
    ({ t: Any => new Boxed94(t) }, classOf[Boxed94[Any]]),
    ({ t: Any => new Boxed95(t) }, classOf[Boxed95[Any]]),
    ({ t: Any => new Boxed96(t) }, classOf[Boxed96[Any]]),
    ({ t: Any => new Boxed97(t) }, classOf[Boxed97[Any]]),
    ({ t: Any => new Boxed98(t) }, classOf[Boxed98[Any]]),
    ({ t: Any => new Boxed99(t) }, classOf[Boxed99[Any]]))

  private[this] val boxes: AtomicReference[List[(Any => Boxed[Any], Class[_ <: Boxed[Any]])]] =
    new AtomicReference(allBoxes)

  def allClasses: Seq[Class[_ <: Boxed[_]]] = allBoxes.map(_._2)

  def next[K]: (K => Boxed[K], Class[Boxed[K]]) = boxes.get match {
    case list @ (h :: tail) if boxes.compareAndSet(list, tail) =>
      h.asInstanceOf[(K => Boxed[K], Class[Boxed[K]])]
    case (h :: tail) => next[K] // Try again
    case Nil => sys.error("Exhausted the boxed classes")
  }
}
