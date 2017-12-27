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

/**
 * This interface is a way of wrapping a value in a marker class
 * whose class identity is used to control which serialization we
 * use. This is an internal implementation detail about how we
 * interact with cascading and hadoop. Users should never care.
 */
trait Boxed[+K] {
  def get: K
}

class Boxed0[K](override val get: K) extends Boxed[K]

class Boxed1[K](override val get: K) extends Boxed[K]

class Boxed2[K](override val get: K) extends Boxed[K]

class Boxed3[K](override val get: K) extends Boxed[K]

class Boxed4[K](override val get: K) extends Boxed[K]

class Boxed5[K](override val get: K) extends Boxed[K]

class Boxed6[K](override val get: K) extends Boxed[K]

class Boxed7[K](override val get: K) extends Boxed[K]

class Boxed8[K](override val get: K) extends Boxed[K]

class Boxed9[K](override val get: K) extends Boxed[K]

class Boxed10[K](override val get: K) extends Boxed[K]

class Boxed11[K](override val get: K) extends Boxed[K]

class Boxed12[K](override val get: K) extends Boxed[K]

class Boxed13[K](override val get: K) extends Boxed[K]

class Boxed14[K](override val get: K) extends Boxed[K]

class Boxed15[K](override val get: K) extends Boxed[K]

class Boxed16[K](override val get: K) extends Boxed[K]

class Boxed17[K](override val get: K) extends Boxed[K]

class Boxed18[K](override val get: K) extends Boxed[K]

class Boxed19[K](override val get: K) extends Boxed[K]

class Boxed20[K](override val get: K) extends Boxed[K]

class Boxed21[K](override val get: K) extends Boxed[K]

class Boxed22[K](override val get: K) extends Boxed[K]

class Boxed23[K](override val get: K) extends Boxed[K]

class Boxed24[K](override val get: K) extends Boxed[K]

class Boxed25[K](override val get: K) extends Boxed[K]

class Boxed26[K](override val get: K) extends Boxed[K]

class Boxed27[K](override val get: K) extends Boxed[K]

class Boxed28[K](override val get: K) extends Boxed[K]

class Boxed29[K](override val get: K) extends Boxed[K]

class Boxed30[K](override val get: K) extends Boxed[K]

class Boxed31[K](override val get: K) extends Boxed[K]

class Boxed32[K](override val get: K) extends Boxed[K]

class Boxed33[K](override val get: K) extends Boxed[K]

class Boxed34[K](override val get: K) extends Boxed[K]

class Boxed35[K](override val get: K) extends Boxed[K]

class Boxed36[K](override val get: K) extends Boxed[K]

class Boxed37[K](override val get: K) extends Boxed[K]

class Boxed38[K](override val get: K) extends Boxed[K]

class Boxed39[K](override val get: K) extends Boxed[K]

class Boxed40[K](override val get: K) extends Boxed[K]

class Boxed41[K](override val get: K) extends Boxed[K]

class Boxed42[K](override val get: K) extends Boxed[K]

class Boxed43[K](override val get: K) extends Boxed[K]

class Boxed44[K](override val get: K) extends Boxed[K]

class Boxed45[K](override val get: K) extends Boxed[K]

class Boxed46[K](override val get: K) extends Boxed[K]

class Boxed47[K](override val get: K) extends Boxed[K]

class Boxed48[K](override val get: K) extends Boxed[K]

class Boxed49[K](override val get: K) extends Boxed[K]

class Boxed50[K](override val get: K) extends Boxed[K]

class Boxed51[K](override val get: K) extends Boxed[K]

class Boxed52[K](override val get: K) extends Boxed[K]

class Boxed53[K](override val get: K) extends Boxed[K]

class Boxed54[K](override val get: K) extends Boxed[K]

class Boxed55[K](override val get: K) extends Boxed[K]

class Boxed56[K](override val get: K) extends Boxed[K]

class Boxed57[K](override val get: K) extends Boxed[K]

class Boxed58[K](override val get: K) extends Boxed[K]

class Boxed59[K](override val get: K) extends Boxed[K]

class Boxed60[K](override val get: K) extends Boxed[K]

class Boxed61[K](override val get: K) extends Boxed[K]

class Boxed62[K](override val get: K) extends Boxed[K]

class Boxed63[K](override val get: K) extends Boxed[K]

class Boxed64[K](override val get: K) extends Boxed[K]

class Boxed65[K](override val get: K) extends Boxed[K]

class Boxed66[K](override val get: K) extends Boxed[K]

class Boxed67[K](override val get: K) extends Boxed[K]

class Boxed68[K](override val get: K) extends Boxed[K]

class Boxed69[K](override val get: K) extends Boxed[K]

class Boxed70[K](override val get: K) extends Boxed[K]

class Boxed71[K](override val get: K) extends Boxed[K]

class Boxed72[K](override val get: K) extends Boxed[K]

class Boxed73[K](override val get: K) extends Boxed[K]

class Boxed74[K](override val get: K) extends Boxed[K]

class Boxed75[K](override val get: K) extends Boxed[K]

class Boxed76[K](override val get: K) extends Boxed[K]

class Boxed77[K](override val get: K) extends Boxed[K]

class Boxed78[K](override val get: K) extends Boxed[K]

class Boxed79[K](override val get: K) extends Boxed[K]

class Boxed80[K](override val get: K) extends Boxed[K]

class Boxed81[K](override val get: K) extends Boxed[K]

class Boxed82[K](override val get: K) extends Boxed[K]

class Boxed83[K](override val get: K) extends Boxed[K]

class Boxed84[K](override val get: K) extends Boxed[K]

class Boxed85[K](override val get: K) extends Boxed[K]

class Boxed86[K](override val get: K) extends Boxed[K]

class Boxed87[K](override val get: K) extends Boxed[K]

class Boxed88[K](override val get: K) extends Boxed[K]

class Boxed89[K](override val get: K) extends Boxed[K]

class Boxed90[K](override val get: K) extends Boxed[K]

class Boxed91[K](override val get: K) extends Boxed[K]

class Boxed92[K](override val get: K) extends Boxed[K]

class Boxed93[K](override val get: K) extends Boxed[K]

class Boxed94[K](override val get: K) extends Boxed[K]

class Boxed95[K](override val get: K) extends Boxed[K]

class Boxed96[K](override val get: K) extends Boxed[K]

class Boxed97[K](override val get: K) extends Boxed[K]

class Boxed98[K](override val get: K) extends Boxed[K]

class Boxed99[K](override val get: K) extends Boxed[K]

class Boxed100[K](override val get: K) extends Boxed[K]

class Boxed101[K](override val get: K) extends Boxed[K]

class Boxed102[K](override val get: K) extends Boxed[K]

class Boxed103[K](override val get: K) extends Boxed[K]

class Boxed104[K](override val get: K) extends Boxed[K]

class Boxed105[K](override val get: K) extends Boxed[K]

class Boxed106[K](override val get: K) extends Boxed[K]

class Boxed107[K](override val get: K) extends Boxed[K]

class Boxed108[K](override val get: K) extends Boxed[K]

class Boxed109[K](override val get: K) extends Boxed[K]

class Boxed110[K](override val get: K) extends Boxed[K]

class Boxed111[K](override val get: K) extends Boxed[K]

class Boxed112[K](override val get: K) extends Boxed[K]

class Boxed113[K](override val get: K) extends Boxed[K]

class Boxed114[K](override val get: K) extends Boxed[K]

class Boxed115[K](override val get: K) extends Boxed[K]

class Boxed116[K](override val get: K) extends Boxed[K]

class Boxed117[K](override val get: K) extends Boxed[K]

class Boxed118[K](override val get: K) extends Boxed[K]

class Boxed119[K](override val get: K) extends Boxed[K]

class Boxed120[K](override val get: K) extends Boxed[K]

class Boxed121[K](override val get: K) extends Boxed[K]

class Boxed122[K](override val get: K) extends Boxed[K]

class Boxed123[K](override val get: K) extends Boxed[K]

class Boxed124[K](override val get: K) extends Boxed[K]

class Boxed125[K](override val get: K) extends Boxed[K]

class Boxed126[K](override val get: K) extends Boxed[K]

class Boxed127[K](override val get: K) extends Boxed[K]

class Boxed128[K](override val get: K) extends Boxed[K]

class Boxed129[K](override val get: K) extends Boxed[K]

class Boxed130[K](override val get: K) extends Boxed[K]

class Boxed131[K](override val get: K) extends Boxed[K]

class Boxed132[K](override val get: K) extends Boxed[K]

class Boxed133[K](override val get: K) extends Boxed[K]

class Boxed134[K](override val get: K) extends Boxed[K]

class Boxed135[K](override val get: K) extends Boxed[K]

class Boxed136[K](override val get: K) extends Boxed[K]

class Boxed137[K](override val get: K) extends Boxed[K]

class Boxed138[K](override val get: K) extends Boxed[K]

class Boxed139[K](override val get: K) extends Boxed[K]

class Boxed140[K](override val get: K) extends Boxed[K]

class Boxed141[K](override val get: K) extends Boxed[K]

class Boxed142[K](override val get: K) extends Boxed[K]

class Boxed143[K](override val get: K) extends Boxed[K]

class Boxed144[K](override val get: K) extends Boxed[K]

class Boxed145[K](override val get: K) extends Boxed[K]

class Boxed146[K](override val get: K) extends Boxed[K]

class Boxed147[K](override val get: K) extends Boxed[K]

class Boxed148[K](override val get: K) extends Boxed[K]

class Boxed149[K](override val get: K) extends Boxed[K]

class Boxed150[K](override val get: K) extends Boxed[K]

class Boxed151[K](override val get: K) extends Boxed[K]

class Boxed152[K](override val get: K) extends Boxed[K]

class Boxed153[K](override val get: K) extends Boxed[K]

class Boxed154[K](override val get: K) extends Boxed[K]

class Boxed155[K](override val get: K) extends Boxed[K]

class Boxed156[K](override val get: K) extends Boxed[K]

class Boxed157[K](override val get: K) extends Boxed[K]

class Boxed158[K](override val get: K) extends Boxed[K]

class Boxed159[K](override val get: K) extends Boxed[K]

class Boxed160[K](override val get: K) extends Boxed[K]

class Boxed161[K](override val get: K) extends Boxed[K]

class Boxed162[K](override val get: K) extends Boxed[K]

class Boxed163[K](override val get: K) extends Boxed[K]

class Boxed164[K](override val get: K) extends Boxed[K]

class Boxed165[K](override val get: K) extends Boxed[K]

class Boxed166[K](override val get: K) extends Boxed[K]

class Boxed167[K](override val get: K) extends Boxed[K]

class Boxed168[K](override val get: K) extends Boxed[K]

class Boxed169[K](override val get: K) extends Boxed[K]

class Boxed170[K](override val get: K) extends Boxed[K]

class Boxed171[K](override val get: K) extends Boxed[K]

class Boxed172[K](override val get: K) extends Boxed[K]

class Boxed173[K](override val get: K) extends Boxed[K]

class Boxed174[K](override val get: K) extends Boxed[K]

class Boxed175[K](override val get: K) extends Boxed[K]

class Boxed176[K](override val get: K) extends Boxed[K]

class Boxed177[K](override val get: K) extends Boxed[K]

class Boxed178[K](override val get: K) extends Boxed[K]

class Boxed179[K](override val get: K) extends Boxed[K]

class Boxed180[K](override val get: K) extends Boxed[K]

class Boxed181[K](override val get: K) extends Boxed[K]

class Boxed182[K](override val get: K) extends Boxed[K]

class Boxed183[K](override val get: K) extends Boxed[K]

class Boxed184[K](override val get: K) extends Boxed[K]

class Boxed185[K](override val get: K) extends Boxed[K]

class Boxed186[K](override val get: K) extends Boxed[K]

class Boxed187[K](override val get: K) extends Boxed[K]

class Boxed188[K](override val get: K) extends Boxed[K]

class Boxed189[K](override val get: K) extends Boxed[K]

class Boxed190[K](override val get: K) extends Boxed[K]

class Boxed191[K](override val get: K) extends Boxed[K]

class Boxed192[K](override val get: K) extends Boxed[K]

class Boxed193[K](override val get: K) extends Boxed[K]

class Boxed194[K](override val get: K) extends Boxed[K]

class Boxed195[K](override val get: K) extends Boxed[K]

class Boxed196[K](override val get: K) extends Boxed[K]

class Boxed197[K](override val get: K) extends Boxed[K]

class Boxed198[K](override val get: K) extends Boxed[K]

class Boxed199[K](override val get: K) extends Boxed[K]

class Boxed200[K](override val get: K) extends Boxed[K]

class Boxed201[K](override val get: K) extends Boxed[K]

class Boxed202[K](override val get: K) extends Boxed[K]

class Boxed203[K](override val get: K) extends Boxed[K]

class Boxed204[K](override val get: K) extends Boxed[K]

class Boxed205[K](override val get: K) extends Boxed[K]

class Boxed206[K](override val get: K) extends Boxed[K]

class Boxed207[K](override val get: K) extends Boxed[K]

class Boxed208[K](override val get: K) extends Boxed[K]

class Boxed209[K](override val get: K) extends Boxed[K]

class Boxed210[K](override val get: K) extends Boxed[K]

class Boxed211[K](override val get: K) extends Boxed[K]

class Boxed212[K](override val get: K) extends Boxed[K]

class Boxed213[K](override val get: K) extends Boxed[K]

class Boxed214[K](override val get: K) extends Boxed[K]

class Boxed215[K](override val get: K) extends Boxed[K]

class Boxed216[K](override val get: K) extends Boxed[K]

class Boxed217[K](override val get: K) extends Boxed[K]

class Boxed218[K](override val get: K) extends Boxed[K]

class Boxed219[K](override val get: K) extends Boxed[K]

class Boxed220[K](override val get: K) extends Boxed[K]

class Boxed221[K](override val get: K) extends Boxed[K]

class Boxed222[K](override val get: K) extends Boxed[K]

class Boxed223[K](override val get: K) extends Boxed[K]

class Boxed224[K](override val get: K) extends Boxed[K]

class Boxed225[K](override val get: K) extends Boxed[K]

class Boxed226[K](override val get: K) extends Boxed[K]

class Boxed227[K](override val get: K) extends Boxed[K]

class Boxed228[K](override val get: K) extends Boxed[K]

class Boxed229[K](override val get: K) extends Boxed[K]

class Boxed230[K](override val get: K) extends Boxed[K]

class Boxed231[K](override val get: K) extends Boxed[K]

class Boxed232[K](override val get: K) extends Boxed[K]

class Boxed233[K](override val get: K) extends Boxed[K]

class Boxed234[K](override val get: K) extends Boxed[K]

class Boxed235[K](override val get: K) extends Boxed[K]

class Boxed236[K](override val get: K) extends Boxed[K]

class Boxed237[K](override val get: K) extends Boxed[K]

class Boxed238[K](override val get: K) extends Boxed[K]

class Boxed239[K](override val get: K) extends Boxed[K]

class Boxed240[K](override val get: K) extends Boxed[K]

class Boxed241[K](override val get: K) extends Boxed[K]

class Boxed242[K](override val get: K) extends Boxed[K]

class Boxed243[K](override val get: K) extends Boxed[K]

class Boxed244[K](override val get: K) extends Boxed[K]

class Boxed245[K](override val get: K) extends Boxed[K]

class Boxed246[K](override val get: K) extends Boxed[K]

class Boxed247[K](override val get: K) extends Boxed[K]

class Boxed248[K](override val get: K) extends Boxed[K]

class Boxed249[K](override val get: K) extends Boxed[K]

class Boxed250[K](override val get: K) extends Boxed[K]

case class BoxedOrderedSerialization[K](box: K => Boxed[K],
  ord: OrderedSerialization[K]) extends OrderedSerialization[Boxed[K]] {

  override def compare(a: Boxed[K], b: Boxed[K]) = ord.compare(a.get, b.get)
  override def hash(k: Boxed[K]) = ord.hash(k.get)
  override def compareBinary(a: InputStream, b: InputStream) = ord.compareBinary(a, b)
  override def read(from: InputStream) = ord.read(from).map(box)
  override def write(into: OutputStream, bk: Boxed[K]) = ord.write(into, bk.get)
  override def staticSize = ord.staticSize
  override def dynamicSize(k: Boxed[K]) = ord.dynamicSize(k.get)
}

// Moving boxed lamdbas into a new object and breaking them up into 2 lists
// to get around this Scala 2.12 bug: https://issues.scala-lang.org/browse/SI-10232
object BoxedLambdas {
  /* You might wonder: "Why not do something a little more type-safe like this?"
   *
   *     private[this] def f0[K](t: K) = new Boxed0(t)
   *     private[this] def f1[K](t: K) = new Boxed1(t)
   *     ...
   *
   *    private[this] def allBoxes[K]: List[(K => Boxed[K], Class[_ <: Boxed[K]])] =
   *      List(
   *        (f0[K](_), classOf[Boxed0[K]]),
   *        (f1[K](_), classOf[Boxed1[K]]),
   *        ...
   *
   * The problem with that is that you can only safely store `val`s in an
   * `AtomicReference`, but `val`s cannot polymorphic, which is the reason for
   * using `Any` here instead of parametrizing everything over a type parameter
   * `K`.
   */
  private[serialization] val boxes1 = List(
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
    ({ t: Any => new Boxed99(t) }, classOf[Boxed99[Any]]),
    ({ t: Any => new Boxed100(t) }, classOf[Boxed100[Any]]),
    ({ t: Any => new Boxed101(t) }, classOf[Boxed101[Any]]),
    ({ t: Any => new Boxed102(t) }, classOf[Boxed102[Any]]),
    ({ t: Any => new Boxed103(t) }, classOf[Boxed103[Any]]),
    ({ t: Any => new Boxed104(t) }, classOf[Boxed104[Any]]),
    ({ t: Any => new Boxed105(t) }, classOf[Boxed105[Any]]),
    ({ t: Any => new Boxed106(t) }, classOf[Boxed106[Any]]),
    ({ t: Any => new Boxed107(t) }, classOf[Boxed107[Any]]),
    ({ t: Any => new Boxed108(t) }, classOf[Boxed108[Any]]),
    ({ t: Any => new Boxed109(t) }, classOf[Boxed109[Any]]),
    ({ t: Any => new Boxed110(t) }, classOf[Boxed110[Any]]),
    ({ t: Any => new Boxed111(t) }, classOf[Boxed111[Any]]),
    ({ t: Any => new Boxed112(t) }, classOf[Boxed112[Any]]),
    ({ t: Any => new Boxed113(t) }, classOf[Boxed113[Any]]),
    ({ t: Any => new Boxed114(t) }, classOf[Boxed114[Any]]),
    ({ t: Any => new Boxed115(t) }, classOf[Boxed115[Any]]),
    ({ t: Any => new Boxed116(t) }, classOf[Boxed116[Any]]),
    ({ t: Any => new Boxed117(t) }, classOf[Boxed117[Any]]),
    ({ t: Any => new Boxed118(t) }, classOf[Boxed118[Any]]),
    ({ t: Any => new Boxed119(t) }, classOf[Boxed119[Any]]),
    ({ t: Any => new Boxed120(t) }, classOf[Boxed120[Any]]),
    ({ t: Any => new Boxed121(t) }, classOf[Boxed121[Any]]),
    ({ t: Any => new Boxed122(t) }, classOf[Boxed122[Any]]),
    ({ t: Any => new Boxed123(t) }, classOf[Boxed123[Any]]),
    ({ t: Any => new Boxed124(t) }, classOf[Boxed124[Any]]))

  private[serialization] val boxes2 = List(
    ({ t: Any => new Boxed125(t) }, classOf[Boxed125[Any]]),
    ({ t: Any => new Boxed126(t) }, classOf[Boxed126[Any]]),
    ({ t: Any => new Boxed127(t) }, classOf[Boxed127[Any]]),
    ({ t: Any => new Boxed128(t) }, classOf[Boxed128[Any]]),
    ({ t: Any => new Boxed129(t) }, classOf[Boxed129[Any]]),
    ({ t: Any => new Boxed130(t) }, classOf[Boxed130[Any]]),
    ({ t: Any => new Boxed131(t) }, classOf[Boxed131[Any]]),
    ({ t: Any => new Boxed132(t) }, classOf[Boxed132[Any]]),
    ({ t: Any => new Boxed133(t) }, classOf[Boxed133[Any]]),
    ({ t: Any => new Boxed134(t) }, classOf[Boxed134[Any]]),
    ({ t: Any => new Boxed135(t) }, classOf[Boxed135[Any]]),
    ({ t: Any => new Boxed136(t) }, classOf[Boxed136[Any]]),
    ({ t: Any => new Boxed137(t) }, classOf[Boxed137[Any]]),
    ({ t: Any => new Boxed138(t) }, classOf[Boxed138[Any]]),
    ({ t: Any => new Boxed139(t) }, classOf[Boxed139[Any]]),
    ({ t: Any => new Boxed140(t) }, classOf[Boxed140[Any]]),
    ({ t: Any => new Boxed141(t) }, classOf[Boxed141[Any]]),
    ({ t: Any => new Boxed142(t) }, classOf[Boxed142[Any]]),
    ({ t: Any => new Boxed143(t) }, classOf[Boxed143[Any]]),
    ({ t: Any => new Boxed144(t) }, classOf[Boxed144[Any]]),
    ({ t: Any => new Boxed145(t) }, classOf[Boxed145[Any]]),
    ({ t: Any => new Boxed146(t) }, classOf[Boxed146[Any]]),
    ({ t: Any => new Boxed147(t) }, classOf[Boxed147[Any]]),
    ({ t: Any => new Boxed148(t) }, classOf[Boxed148[Any]]),
    ({ t: Any => new Boxed149(t) }, classOf[Boxed149[Any]]),
    ({ t: Any => new Boxed150(t) }, classOf[Boxed150[Any]]),
    ({ t: Any => new Boxed151(t) }, classOf[Boxed151[Any]]),
    ({ t: Any => new Boxed152(t) }, classOf[Boxed152[Any]]),
    ({ t: Any => new Boxed153(t) }, classOf[Boxed153[Any]]),
    ({ t: Any => new Boxed154(t) }, classOf[Boxed154[Any]]),
    ({ t: Any => new Boxed155(t) }, classOf[Boxed155[Any]]),
    ({ t: Any => new Boxed156(t) }, classOf[Boxed156[Any]]),
    ({ t: Any => new Boxed157(t) }, classOf[Boxed157[Any]]),
    ({ t: Any => new Boxed158(t) }, classOf[Boxed158[Any]]),
    ({ t: Any => new Boxed159(t) }, classOf[Boxed159[Any]]),
    ({ t: Any => new Boxed160(t) }, classOf[Boxed160[Any]]),
    ({ t: Any => new Boxed161(t) }, classOf[Boxed161[Any]]),
    ({ t: Any => new Boxed162(t) }, classOf[Boxed162[Any]]),
    ({ t: Any => new Boxed163(t) }, classOf[Boxed163[Any]]),
    ({ t: Any => new Boxed164(t) }, classOf[Boxed164[Any]]),
    ({ t: Any => new Boxed165(t) }, classOf[Boxed165[Any]]),
    ({ t: Any => new Boxed166(t) }, classOf[Boxed166[Any]]),
    ({ t: Any => new Boxed167(t) }, classOf[Boxed167[Any]]),
    ({ t: Any => new Boxed168(t) }, classOf[Boxed168[Any]]),
    ({ t: Any => new Boxed169(t) }, classOf[Boxed169[Any]]),
    ({ t: Any => new Boxed170(t) }, classOf[Boxed170[Any]]),
    ({ t: Any => new Boxed171(t) }, classOf[Boxed171[Any]]),
    ({ t: Any => new Boxed172(t) }, classOf[Boxed172[Any]]),
    ({ t: Any => new Boxed173(t) }, classOf[Boxed173[Any]]),
    ({ t: Any => new Boxed174(t) }, classOf[Boxed174[Any]]),
    ({ t: Any => new Boxed175(t) }, classOf[Boxed175[Any]]),
    ({ t: Any => new Boxed176(t) }, classOf[Boxed176[Any]]),
    ({ t: Any => new Boxed177(t) }, classOf[Boxed177[Any]]),
    ({ t: Any => new Boxed178(t) }, classOf[Boxed178[Any]]),
    ({ t: Any => new Boxed179(t) }, classOf[Boxed179[Any]]),
    ({ t: Any => new Boxed180(t) }, classOf[Boxed180[Any]]),
    ({ t: Any => new Boxed181(t) }, classOf[Boxed181[Any]]),
    ({ t: Any => new Boxed182(t) }, classOf[Boxed182[Any]]),
    ({ t: Any => new Boxed183(t) }, classOf[Boxed183[Any]]),
    ({ t: Any => new Boxed184(t) }, classOf[Boxed184[Any]]),
    ({ t: Any => new Boxed185(t) }, classOf[Boxed185[Any]]),
    ({ t: Any => new Boxed186(t) }, classOf[Boxed186[Any]]),
    ({ t: Any => new Boxed187(t) }, classOf[Boxed187[Any]]),
    ({ t: Any => new Boxed188(t) }, classOf[Boxed188[Any]]),
    ({ t: Any => new Boxed189(t) }, classOf[Boxed189[Any]]),
    ({ t: Any => new Boxed190(t) }, classOf[Boxed190[Any]]),
    ({ t: Any => new Boxed191(t) }, classOf[Boxed191[Any]]),
    ({ t: Any => new Boxed192(t) }, classOf[Boxed192[Any]]),
    ({ t: Any => new Boxed193(t) }, classOf[Boxed193[Any]]),
    ({ t: Any => new Boxed194(t) }, classOf[Boxed194[Any]]),
    ({ t: Any => new Boxed195(t) }, classOf[Boxed195[Any]]),
    ({ t: Any => new Boxed196(t) }, classOf[Boxed196[Any]]),
    ({ t: Any => new Boxed197(t) }, classOf[Boxed197[Any]]),
    ({ t: Any => new Boxed198(t) }, classOf[Boxed198[Any]]),
    ({ t: Any => new Boxed199(t) }, classOf[Boxed199[Any]]),
    ({ t: Any => new Boxed200(t) }, classOf[Boxed200[Any]]),
    ({ t: Any => new Boxed201(t) }, classOf[Boxed201[Any]]),
    ({ t: Any => new Boxed202(t) }, classOf[Boxed202[Any]]),
    ({ t: Any => new Boxed203(t) }, classOf[Boxed203[Any]]),
    ({ t: Any => new Boxed204(t) }, classOf[Boxed204[Any]]),
    ({ t: Any => new Boxed205(t) }, classOf[Boxed205[Any]]),
    ({ t: Any => new Boxed206(t) }, classOf[Boxed206[Any]]),
    ({ t: Any => new Boxed207(t) }, classOf[Boxed207[Any]]),
    ({ t: Any => new Boxed208(t) }, classOf[Boxed208[Any]]),
    ({ t: Any => new Boxed209(t) }, classOf[Boxed209[Any]]),
    ({ t: Any => new Boxed210(t) }, classOf[Boxed210[Any]]),
    ({ t: Any => new Boxed211(t) }, classOf[Boxed211[Any]]),
    ({ t: Any => new Boxed212(t) }, classOf[Boxed212[Any]]),
    ({ t: Any => new Boxed213(t) }, classOf[Boxed213[Any]]),
    ({ t: Any => new Boxed214(t) }, classOf[Boxed214[Any]]),
    ({ t: Any => new Boxed215(t) }, classOf[Boxed215[Any]]),
    ({ t: Any => new Boxed216(t) }, classOf[Boxed216[Any]]),
    ({ t: Any => new Boxed217(t) }, classOf[Boxed217[Any]]),
    ({ t: Any => new Boxed218(t) }, classOf[Boxed218[Any]]),
    ({ t: Any => new Boxed219(t) }, classOf[Boxed219[Any]]),
    ({ t: Any => new Boxed220(t) }, classOf[Boxed220[Any]]),
    ({ t: Any => new Boxed221(t) }, classOf[Boxed221[Any]]),
    ({ t: Any => new Boxed222(t) }, classOf[Boxed222[Any]]),
    ({ t: Any => new Boxed223(t) }, classOf[Boxed223[Any]]),
    ({ t: Any => new Boxed224(t) }, classOf[Boxed224[Any]]),
    ({ t: Any => new Boxed225(t) }, classOf[Boxed225[Any]]),
    ({ t: Any => new Boxed226(t) }, classOf[Boxed226[Any]]),
    ({ t: Any => new Boxed227(t) }, classOf[Boxed227[Any]]),
    ({ t: Any => new Boxed228(t) }, classOf[Boxed228[Any]]),
    ({ t: Any => new Boxed229(t) }, classOf[Boxed229[Any]]),
    ({ t: Any => new Boxed230(t) }, classOf[Boxed230[Any]]),
    ({ t: Any => new Boxed231(t) }, classOf[Boxed231[Any]]),
    ({ t: Any => new Boxed232(t) }, classOf[Boxed232[Any]]),
    ({ t: Any => new Boxed233(t) }, classOf[Boxed233[Any]]),
    ({ t: Any => new Boxed234(t) }, classOf[Boxed234[Any]]),
    ({ t: Any => new Boxed235(t) }, classOf[Boxed235[Any]]),
    ({ t: Any => new Boxed236(t) }, classOf[Boxed236[Any]]),
    ({ t: Any => new Boxed237(t) }, classOf[Boxed237[Any]]),
    ({ t: Any => new Boxed238(t) }, classOf[Boxed238[Any]]),
    ({ t: Any => new Boxed239(t) }, classOf[Boxed239[Any]]),
    ({ t: Any => new Boxed240(t) }, classOf[Boxed240[Any]]),
    ({ t: Any => new Boxed241(t) }, classOf[Boxed241[Any]]),
    ({ t: Any => new Boxed242(t) }, classOf[Boxed242[Any]]),
    ({ t: Any => new Boxed243(t) }, classOf[Boxed243[Any]]),
    ({ t: Any => new Boxed244(t) }, classOf[Boxed244[Any]]),
    ({ t: Any => new Boxed245(t) }, classOf[Boxed245[Any]]),
    ({ t: Any => new Boxed246(t) }, classOf[Boxed246[Any]]),
    ({ t: Any => new Boxed247(t) }, classOf[Boxed247[Any]]),
    ({ t: Any => new Boxed248(t) }, classOf[Boxed248[Any]]),
    ({ t: Any => new Boxed249(t) }, classOf[Boxed249[Any]]),
    ({ t: Any => new Boxed250(t) }, classOf[Boxed250[Any]]))
}

object Boxed {
  import BoxedLambdas._

  private[this] val allBoxes = boxes1 ++ boxes2

  private[this] val boxes: AtomicReference[List[(Any => Boxed[Any], Class[_ <: Boxed[Any]])]] =
    new AtomicReference(allBoxes)

  def allClasses: Seq[Class[_ <: Boxed[_]]] = allBoxes.map(_._2)

  private[this] val boxedCache = new java.util.concurrent.ConcurrentHashMap[AnyRef, (Any => Boxed[Any], Class[Boxed[Any]])]()

  private[scalding] def nextCached[K](cacheKey: Option[AnyRef]): (K => Boxed[K], Class[Boxed[K]]) =
    cacheKey match {
      case Some(cls) =>
        val untypedRes = Option(boxedCache.get(cls)) match {
          case Some(r) => r
          case None =>
            val r = next[Any]()
            boxedCache.putIfAbsent(cls, r)
            r
        }
        untypedRes.asInstanceOf[(K => Boxed[K], Class[Boxed[K]])]
      case None => next[K]()
    }

  def next[K](): (K => Boxed[K], Class[Boxed[K]]) = boxes.get match {
    case list @ (h :: tail) if boxes.compareAndSet(list, tail) =>
      h.asInstanceOf[(K => Boxed[K], Class[Boxed[K]])]
    case (h :: tail) => next[K]() // Try again
    case Nil => sys.error(
      """|Scalding's ordered serialization logic exhausted the finite supply of boxed classes.
         |
         |Explanation: Scalding's ordered serialization logic internally uses
         |a large, but fixed, supply of unique wrapper types to box values in
         |order to control which serialization is used.  Exhausting this supply
         |means that you happen to have a very complex Scalding job that uses
         |ordered serialization for a very large number of diverse types""".stripMargin)
  }
}
