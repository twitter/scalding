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

object Boxed {
  private[this] def f0[K](t: K) = new Boxed0(t)
  private[this] def f1[K](t: K) = new Boxed1(t)
  private[this] def f2[K](t: K) = new Boxed2(t)
  private[this] def f3[K](t: K) = new Boxed3(t)
  private[this] def f4[K](t: K) = new Boxed4(t)
  private[this] def f5[K](t: K) = new Boxed5(t)
  private[this] def f6[K](t: K) = new Boxed6(t)
  private[this] def f7[K](t: K) = new Boxed7(t)
  private[this] def f8[K](t: K) = new Boxed8(t)
  private[this] def f9[K](t: K) = new Boxed9(t)
  private[this] def f10[K](t: K) = new Boxed10(t)
  private[this] def f11[K](t: K) = new Boxed11(t)
  private[this] def f12[K](t: K) = new Boxed12(t)
  private[this] def f13[K](t: K) = new Boxed13(t)
  private[this] def f14[K](t: K) = new Boxed14(t)
  private[this] def f15[K](t: K) = new Boxed15(t)
  private[this] def f16[K](t: K) = new Boxed16(t)
  private[this] def f17[K](t: K) = new Boxed17(t)
  private[this] def f18[K](t: K) = new Boxed18(t)
  private[this] def f19[K](t: K) = new Boxed19(t)
  private[this] def f20[K](t: K) = new Boxed20(t)
  private[this] def f21[K](t: K) = new Boxed21(t)
  private[this] def f22[K](t: K) = new Boxed22(t)
  private[this] def f23[K](t: K) = new Boxed23(t)
  private[this] def f24[K](t: K) = new Boxed24(t)
  private[this] def f25[K](t: K) = new Boxed25(t)
  private[this] def f26[K](t: K) = new Boxed26(t)
  private[this] def f27[K](t: K) = new Boxed27(t)
  private[this] def f28[K](t: K) = new Boxed28(t)
  private[this] def f29[K](t: K) = new Boxed29(t)
  private[this] def f30[K](t: K) = new Boxed30(t)
  private[this] def f31[K](t: K) = new Boxed31(t)
  private[this] def f32[K](t: K) = new Boxed32(t)
  private[this] def f33[K](t: K) = new Boxed33(t)
  private[this] def f34[K](t: K) = new Boxed34(t)
  private[this] def f35[K](t: K) = new Boxed35(t)
  private[this] def f36[K](t: K) = new Boxed36(t)
  private[this] def f37[K](t: K) = new Boxed37(t)
  private[this] def f38[K](t: K) = new Boxed38(t)
  private[this] def f39[K](t: K) = new Boxed39(t)
  private[this] def f40[K](t: K) = new Boxed40(t)
  private[this] def f41[K](t: K) = new Boxed41(t)
  private[this] def f42[K](t: K) = new Boxed42(t)
  private[this] def f43[K](t: K) = new Boxed43(t)
  private[this] def f44[K](t: K) = new Boxed44(t)
  private[this] def f45[K](t: K) = new Boxed45(t)
  private[this] def f46[K](t: K) = new Boxed46(t)
  private[this] def f47[K](t: K) = new Boxed47(t)
  private[this] def f48[K](t: K) = new Boxed48(t)
  private[this] def f49[K](t: K) = new Boxed49(t)
  private[this] def f50[K](t: K) = new Boxed50(t)
  private[this] def f51[K](t: K) = new Boxed51(t)
  private[this] def f52[K](t: K) = new Boxed52(t)
  private[this] def f53[K](t: K) = new Boxed53(t)
  private[this] def f54[K](t: K) = new Boxed54(t)
  private[this] def f55[K](t: K) = new Boxed55(t)
  private[this] def f56[K](t: K) = new Boxed56(t)
  private[this] def f57[K](t: K) = new Boxed57(t)
  private[this] def f58[K](t: K) = new Boxed58(t)
  private[this] def f59[K](t: K) = new Boxed59(t)
  private[this] def f60[K](t: K) = new Boxed60(t)
  private[this] def f61[K](t: K) = new Boxed61(t)
  private[this] def f62[K](t: K) = new Boxed62(t)
  private[this] def f63[K](t: K) = new Boxed63(t)
  private[this] def f64[K](t: K) = new Boxed64(t)
  private[this] def f65[K](t: K) = new Boxed65(t)
  private[this] def f66[K](t: K) = new Boxed66(t)
  private[this] def f67[K](t: K) = new Boxed67(t)
  private[this] def f68[K](t: K) = new Boxed68(t)
  private[this] def f69[K](t: K) = new Boxed69(t)
  private[this] def f70[K](t: K) = new Boxed70(t)
  private[this] def f71[K](t: K) = new Boxed71(t)
  private[this] def f72[K](t: K) = new Boxed72(t)
  private[this] def f73[K](t: K) = new Boxed73(t)
  private[this] def f74[K](t: K) = new Boxed74(t)
  private[this] def f75[K](t: K) = new Boxed75(t)
  private[this] def f76[K](t: K) = new Boxed76(t)
  private[this] def f77[K](t: K) = new Boxed77(t)
  private[this] def f78[K](t: K) = new Boxed78(t)
  private[this] def f79[K](t: K) = new Boxed79(t)
  private[this] def f80[K](t: K) = new Boxed80(t)
  private[this] def f81[K](t: K) = new Boxed81(t)
  private[this] def f82[K](t: K) = new Boxed82(t)
  private[this] def f83[K](t: K) = new Boxed83(t)
  private[this] def f84[K](t: K) = new Boxed84(t)
  private[this] def f85[K](t: K) = new Boxed85(t)
  private[this] def f86[K](t: K) = new Boxed86(t)
  private[this] def f87[K](t: K) = new Boxed87(t)
  private[this] def f88[K](t: K) = new Boxed88(t)
  private[this] def f89[K](t: K) = new Boxed89(t)
  private[this] def f90[K](t: K) = new Boxed90(t)
  private[this] def f91[K](t: K) = new Boxed91(t)
  private[this] def f92[K](t: K) = new Boxed92(t)
  private[this] def f93[K](t: K) = new Boxed93(t)
  private[this] def f94[K](t: K) = new Boxed94(t)
  private[this] def f95[K](t: K) = new Boxed95(t)
  private[this] def f96[K](t: K) = new Boxed96(t)
  private[this] def f97[K](t: K) = new Boxed97(t)
  private[this] def f98[K](t: K) = new Boxed98(t)
  private[this] def f99[K](t: K) = new Boxed99(t)
  private[this] def f100[K](t: K) = new Boxed100(t)
  private[this] def f101[K](t: K) = new Boxed101(t)
  private[this] def f102[K](t: K) = new Boxed102(t)
  private[this] def f103[K](t: K) = new Boxed103(t)
  private[this] def f104[K](t: K) = new Boxed104(t)
  private[this] def f105[K](t: K) = new Boxed105(t)
  private[this] def f106[K](t: K) = new Boxed106(t)
  private[this] def f107[K](t: K) = new Boxed107(t)
  private[this] def f108[K](t: K) = new Boxed108(t)
  private[this] def f109[K](t: K) = new Boxed109(t)
  private[this] def f110[K](t: K) = new Boxed110(t)
  private[this] def f111[K](t: K) = new Boxed111(t)
  private[this] def f112[K](t: K) = new Boxed112(t)
  private[this] def f113[K](t: K) = new Boxed113(t)
  private[this] def f114[K](t: K) = new Boxed114(t)
  private[this] def f115[K](t: K) = new Boxed115(t)
  private[this] def f116[K](t: K) = new Boxed116(t)
  private[this] def f117[K](t: K) = new Boxed117(t)
  private[this] def f118[K](t: K) = new Boxed118(t)
  private[this] def f119[K](t: K) = new Boxed119(t)
  private[this] def f120[K](t: K) = new Boxed120(t)
  private[this] def f121[K](t: K) = new Boxed121(t)
  private[this] def f122[K](t: K) = new Boxed122(t)
  private[this] def f123[K](t: K) = new Boxed123(t)
  private[this] def f124[K](t: K) = new Boxed124(t)
  private[this] def f125[K](t: K) = new Boxed125(t)
  private[this] def f126[K](t: K) = new Boxed126(t)
  private[this] def f127[K](t: K) = new Boxed127(t)
  private[this] def f128[K](t: K) = new Boxed128(t)
  private[this] def f129[K](t: K) = new Boxed129(t)
  private[this] def f130[K](t: K) = new Boxed130(t)
  private[this] def f131[K](t: K) = new Boxed131(t)
  private[this] def f132[K](t: K) = new Boxed132(t)
  private[this] def f133[K](t: K) = new Boxed133(t)
  private[this] def f134[K](t: K) = new Boxed134(t)
  private[this] def f135[K](t: K) = new Boxed135(t)
  private[this] def f136[K](t: K) = new Boxed136(t)
  private[this] def f137[K](t: K) = new Boxed137(t)
  private[this] def f138[K](t: K) = new Boxed138(t)
  private[this] def f139[K](t: K) = new Boxed139(t)
  private[this] def f140[K](t: K) = new Boxed140(t)
  private[this] def f141[K](t: K) = new Boxed141(t)
  private[this] def f142[K](t: K) = new Boxed142(t)
  private[this] def f143[K](t: K) = new Boxed143(t)
  private[this] def f144[K](t: K) = new Boxed144(t)
  private[this] def f145[K](t: K) = new Boxed145(t)
  private[this] def f146[K](t: K) = new Boxed146(t)
  private[this] def f147[K](t: K) = new Boxed147(t)
  private[this] def f148[K](t: K) = new Boxed148(t)
  private[this] def f149[K](t: K) = new Boxed149(t)
  private[this] def f150[K](t: K) = new Boxed150(t)
  private[this] def f151[K](t: K) = new Boxed151(t)
  private[this] def f152[K](t: K) = new Boxed152(t)
  private[this] def f153[K](t: K) = new Boxed153(t)
  private[this] def f154[K](t: K) = new Boxed154(t)
  private[this] def f155[K](t: K) = new Boxed155(t)
  private[this] def f156[K](t: K) = new Boxed156(t)
  private[this] def f157[K](t: K) = new Boxed157(t)
  private[this] def f158[K](t: K) = new Boxed158(t)
  private[this] def f159[K](t: K) = new Boxed159(t)
  private[this] def f160[K](t: K) = new Boxed160(t)
  private[this] def f161[K](t: K) = new Boxed161(t)
  private[this] def f162[K](t: K) = new Boxed162(t)
  private[this] def f163[K](t: K) = new Boxed163(t)
  private[this] def f164[K](t: K) = new Boxed164(t)
  private[this] def f165[K](t: K) = new Boxed165(t)
  private[this] def f166[K](t: K) = new Boxed166(t)
  private[this] def f167[K](t: K) = new Boxed167(t)
  private[this] def f168[K](t: K) = new Boxed168(t)
  private[this] def f169[K](t: K) = new Boxed169(t)
  private[this] def f170[K](t: K) = new Boxed170(t)
  private[this] def f171[K](t: K) = new Boxed171(t)
  private[this] def f172[K](t: K) = new Boxed172(t)
  private[this] def f173[K](t: K) = new Boxed173(t)
  private[this] def f174[K](t: K) = new Boxed174(t)
  private[this] def f175[K](t: K) = new Boxed175(t)
  private[this] def f176[K](t: K) = new Boxed176(t)
  private[this] def f177[K](t: K) = new Boxed177(t)
  private[this] def f178[K](t: K) = new Boxed178(t)
  private[this] def f179[K](t: K) = new Boxed179(t)
  private[this] def f180[K](t: K) = new Boxed180(t)
  private[this] def f181[K](t: K) = new Boxed181(t)
  private[this] def f182[K](t: K) = new Boxed182(t)
  private[this] def f183[K](t: K) = new Boxed183(t)
  private[this] def f184[K](t: K) = new Boxed184(t)
  private[this] def f185[K](t: K) = new Boxed185(t)
  private[this] def f186[K](t: K) = new Boxed186(t)
  private[this] def f187[K](t: K) = new Boxed187(t)
  private[this] def f188[K](t: K) = new Boxed188(t)
  private[this] def f189[K](t: K) = new Boxed189(t)
  private[this] def f190[K](t: K) = new Boxed190(t)
  private[this] def f191[K](t: K) = new Boxed191(t)
  private[this] def f192[K](t: K) = new Boxed192(t)
  private[this] def f193[K](t: K) = new Boxed193(t)
  private[this] def f194[K](t: K) = new Boxed194(t)
  private[this] def f195[K](t: K) = new Boxed195(t)
  private[this] def f196[K](t: K) = new Boxed196(t)
  private[this] def f197[K](t: K) = new Boxed197(t)
  private[this] def f198[K](t: K) = new Boxed198(t)
  private[this] def f199[K](t: K) = new Boxed199(t)
  private[this] def f200[K](t: K) = new Boxed200(t)
  private[this] def f201[K](t: K) = new Boxed201(t)
  private[this] def f202[K](t: K) = new Boxed202(t)
  private[this] def f203[K](t: K) = new Boxed203(t)
  private[this] def f204[K](t: K) = new Boxed204(t)
  private[this] def f205[K](t: K) = new Boxed205(t)
  private[this] def f206[K](t: K) = new Boxed206(t)
  private[this] def f207[K](t: K) = new Boxed207(t)
  private[this] def f208[K](t: K) = new Boxed208(t)
  private[this] def f209[K](t: K) = new Boxed209(t)
  private[this] def f210[K](t: K) = new Boxed210(t)
  private[this] def f211[K](t: K) = new Boxed211(t)
  private[this] def f212[K](t: K) = new Boxed212(t)
  private[this] def f213[K](t: K) = new Boxed213(t)
  private[this] def f214[K](t: K) = new Boxed214(t)
  private[this] def f215[K](t: K) = new Boxed215(t)
  private[this] def f216[K](t: K) = new Boxed216(t)
  private[this] def f217[K](t: K) = new Boxed217(t)
  private[this] def f218[K](t: K) = new Boxed218(t)
  private[this] def f219[K](t: K) = new Boxed219(t)
  private[this] def f220[K](t: K) = new Boxed220(t)
  private[this] def f221[K](t: K) = new Boxed221(t)
  private[this] def f222[K](t: K) = new Boxed222(t)
  private[this] def f223[K](t: K) = new Boxed223(t)
  private[this] def f224[K](t: K) = new Boxed224(t)
  private[this] def f225[K](t: K) = new Boxed225(t)
  private[this] def f226[K](t: K) = new Boxed226(t)
  private[this] def f227[K](t: K) = new Boxed227(t)
  private[this] def f228[K](t: K) = new Boxed228(t)
  private[this] def f229[K](t: K) = new Boxed229(t)
  private[this] def f230[K](t: K) = new Boxed230(t)
  private[this] def f231[K](t: K) = new Boxed231(t)
  private[this] def f232[K](t: K) = new Boxed232(t)
  private[this] def f233[K](t: K) = new Boxed233(t)
  private[this] def f234[K](t: K) = new Boxed234(t)
  private[this] def f235[K](t: K) = new Boxed235(t)
  private[this] def f236[K](t: K) = new Boxed236(t)
  private[this] def f237[K](t: K) = new Boxed237(t)
  private[this] def f238[K](t: K) = new Boxed238(t)
  private[this] def f239[K](t: K) = new Boxed239(t)
  private[this] def f240[K](t: K) = new Boxed240(t)
  private[this] def f241[K](t: K) = new Boxed241(t)
  private[this] def f242[K](t: K) = new Boxed242(t)
  private[this] def f243[K](t: K) = new Boxed243(t)
  private[this] def f244[K](t: K) = new Boxed244(t)
  private[this] def f245[K](t: K) = new Boxed245(t)
  private[this] def f246[K](t: K) = new Boxed246(t)
  private[this] def f247[K](t: K) = new Boxed247(t)
  private[this] def f248[K](t: K) = new Boxed248(t)
  private[this] def f249[K](t: K) = new Boxed249(t)
  private[this] def f250[K](t: K) = new Boxed250(t)

  private[this] def allBoxes[K]: List[(K => Boxed[K], Class[_ <: Boxed[K]])] =
    List(
      (f0[K](_), classOf[Boxed0[K]]),
      (f1[K](_), classOf[Boxed1[K]]),
      (f2[K](_), classOf[Boxed2[K]]),
      (f3[K](_), classOf[Boxed3[K]]),
      (f4[K](_), classOf[Boxed4[K]]),
      (f5[K](_), classOf[Boxed5[K]]),
      (f6[K](_), classOf[Boxed6[K]]),
      (f7[K](_), classOf[Boxed7[K]]),
      (f8[K](_), classOf[Boxed8[K]]),
      (f9[K](_), classOf[Boxed9[K]]),
      (f10[K](_), classOf[Boxed10[K]]),
      (f11[K](_), classOf[Boxed11[K]]),
      (f12[K](_), classOf[Boxed12[K]]),
      (f13[K](_), classOf[Boxed13[K]]),
      (f14[K](_), classOf[Boxed14[K]]),
      (f15[K](_), classOf[Boxed15[K]]),
      (f16[K](_), classOf[Boxed16[K]]),
      (f17[K](_), classOf[Boxed17[K]]),
      (f18[K](_), classOf[Boxed18[K]]),
      (f19[K](_), classOf[Boxed19[K]]),
      (f20[K](_), classOf[Boxed20[K]]),
      (f21[K](_), classOf[Boxed21[K]]),
      (f22[K](_), classOf[Boxed22[K]]),
      (f23[K](_), classOf[Boxed23[K]]),
      (f24[K](_), classOf[Boxed24[K]]),
      (f25[K](_), classOf[Boxed25[K]]),
      (f26[K](_), classOf[Boxed26[K]]),
      (f27[K](_), classOf[Boxed27[K]]),
      (f28[K](_), classOf[Boxed28[K]]),
      (f29[K](_), classOf[Boxed29[K]]),
      (f30[K](_), classOf[Boxed30[K]]),
      (f31[K](_), classOf[Boxed31[K]]),
      (f32[K](_), classOf[Boxed32[K]]),
      (f33[K](_), classOf[Boxed33[K]]),
      (f34[K](_), classOf[Boxed34[K]]),
      (f35[K](_), classOf[Boxed35[K]]),
      (f36[K](_), classOf[Boxed36[K]]),
      (f37[K](_), classOf[Boxed37[K]]),
      (f38[K](_), classOf[Boxed38[K]]),
      (f39[K](_), classOf[Boxed39[K]]),
      (f40[K](_), classOf[Boxed40[K]]),
      (f41[K](_), classOf[Boxed41[K]]),
      (f42[K](_), classOf[Boxed42[K]]),
      (f43[K](_), classOf[Boxed43[K]]),
      (f44[K](_), classOf[Boxed44[K]]),
      (f45[K](_), classOf[Boxed45[K]]),
      (f46[K](_), classOf[Boxed46[K]]),
      (f47[K](_), classOf[Boxed47[K]]),
      (f48[K](_), classOf[Boxed48[K]]),
      (f49[K](_), classOf[Boxed49[K]]),
      (f50[K](_), classOf[Boxed50[K]]),
      (f51[K](_), classOf[Boxed51[K]]),
      (f52[K](_), classOf[Boxed52[K]]),
      (f53[K](_), classOf[Boxed53[K]]),
      (f54[K](_), classOf[Boxed54[K]]),
      (f55[K](_), classOf[Boxed55[K]]),
      (f56[K](_), classOf[Boxed56[K]]),
      (f57[K](_), classOf[Boxed57[K]]),
      (f58[K](_), classOf[Boxed58[K]]),
      (f59[K](_), classOf[Boxed59[K]]),
      (f60[K](_), classOf[Boxed60[K]]),
      (f61[K](_), classOf[Boxed61[K]]),
      (f62[K](_), classOf[Boxed62[K]]),
      (f63[K](_), classOf[Boxed63[K]]),
      (f64[K](_), classOf[Boxed64[K]]),
      (f65[K](_), classOf[Boxed65[K]]),
      (f66[K](_), classOf[Boxed66[K]]),
      (f67[K](_), classOf[Boxed67[K]]),
      (f68[K](_), classOf[Boxed68[K]]),
      (f69[K](_), classOf[Boxed69[K]]),
      (f70[K](_), classOf[Boxed70[K]]),
      (f71[K](_), classOf[Boxed71[K]]),
      (f72[K](_), classOf[Boxed72[K]]),
      (f73[K](_), classOf[Boxed73[K]]),
      (f74[K](_), classOf[Boxed74[K]]),
      (f75[K](_), classOf[Boxed75[K]]),
      (f76[K](_), classOf[Boxed76[K]]),
      (f77[K](_), classOf[Boxed77[K]]),
      (f78[K](_), classOf[Boxed78[K]]),
      (f79[K](_), classOf[Boxed79[K]]),
      (f80[K](_), classOf[Boxed80[K]]),
      (f81[K](_), classOf[Boxed81[K]]),
      (f82[K](_), classOf[Boxed82[K]]),
      (f83[K](_), classOf[Boxed83[K]]),
      (f84[K](_), classOf[Boxed84[K]]),
      (f85[K](_), classOf[Boxed85[K]]),
      (f86[K](_), classOf[Boxed86[K]]),
      (f87[K](_), classOf[Boxed87[K]]),
      (f88[K](_), classOf[Boxed88[K]]),
      (f89[K](_), classOf[Boxed89[K]]),
      (f90[K](_), classOf[Boxed90[K]]),
      (f91[K](_), classOf[Boxed91[K]]),
      (f92[K](_), classOf[Boxed92[K]]),
      (f93[K](_), classOf[Boxed93[K]]),
      (f94[K](_), classOf[Boxed94[K]]),
      (f95[K](_), classOf[Boxed95[K]]),
      (f96[K](_), classOf[Boxed96[K]]),
      (f97[K](_), classOf[Boxed97[K]]),
      (f98[K](_), classOf[Boxed98[K]]),
      (f99[K](_), classOf[Boxed99[K]]),
      (f100[K](_), classOf[Boxed100[K]]),
      (f101[K](_), classOf[Boxed101[K]]),
      (f102[K](_), classOf[Boxed102[K]]),
      (f103[K](_), classOf[Boxed103[K]]),
      (f104[K](_), classOf[Boxed104[K]]),
      (f105[K](_), classOf[Boxed105[K]]),
      (f106[K](_), classOf[Boxed106[K]]),
      (f107[K](_), classOf[Boxed107[K]]),
      (f108[K](_), classOf[Boxed108[K]]),
      (f109[K](_), classOf[Boxed109[K]]),
      (f110[K](_), classOf[Boxed110[K]]),
      (f111[K](_), classOf[Boxed111[K]]),
      (f112[K](_), classOf[Boxed112[K]]),
      (f113[K](_), classOf[Boxed113[K]]),
      (f114[K](_), classOf[Boxed114[K]]),
      (f115[K](_), classOf[Boxed115[K]]),
      (f116[K](_), classOf[Boxed116[K]]),
      (f117[K](_), classOf[Boxed117[K]]),
      (f118[K](_), classOf[Boxed118[K]]),
      (f119[K](_), classOf[Boxed119[K]]),
      (f120[K](_), classOf[Boxed120[K]]),
      (f121[K](_), classOf[Boxed121[K]]),
      (f122[K](_), classOf[Boxed122[K]]),
      (f123[K](_), classOf[Boxed123[K]]),
      (f124[K](_), classOf[Boxed124[K]]),
      (f125[K](_), classOf[Boxed125[K]]),
      (f126[K](_), classOf[Boxed126[K]]),
      (f127[K](_), classOf[Boxed127[K]]),
      (f128[K](_), classOf[Boxed128[K]]),
      (f129[K](_), classOf[Boxed129[K]]),
      (f130[K](_), classOf[Boxed130[K]]),
      (f131[K](_), classOf[Boxed131[K]]),
      (f132[K](_), classOf[Boxed132[K]]),
      (f133[K](_), classOf[Boxed133[K]]),
      (f134[K](_), classOf[Boxed134[K]]),
      (f135[K](_), classOf[Boxed135[K]]),
      (f136[K](_), classOf[Boxed136[K]]),
      (f137[K](_), classOf[Boxed137[K]]),
      (f138[K](_), classOf[Boxed138[K]]),
      (f139[K](_), classOf[Boxed139[K]]),
      (f140[K](_), classOf[Boxed140[K]]),
      (f141[K](_), classOf[Boxed141[K]]),
      (f142[K](_), classOf[Boxed142[K]]),
      (f143[K](_), classOf[Boxed143[K]]),
      (f144[K](_), classOf[Boxed144[K]]),
      (f145[K](_), classOf[Boxed145[K]]),
      (f146[K](_), classOf[Boxed146[K]]),
      (f147[K](_), classOf[Boxed147[K]]),
      (f148[K](_), classOf[Boxed148[K]]),
      (f149[K](_), classOf[Boxed149[K]]),
      (f150[K](_), classOf[Boxed150[K]]),
      (f151[K](_), classOf[Boxed151[K]]),
      (f152[K](_), classOf[Boxed152[K]]),
      (f153[K](_), classOf[Boxed153[K]]),
      (f154[K](_), classOf[Boxed154[K]]),
      (f155[K](_), classOf[Boxed155[K]]),
      (f156[K](_), classOf[Boxed156[K]]),
      (f157[K](_), classOf[Boxed157[K]]),
      (f158[K](_), classOf[Boxed158[K]]),
      (f159[K](_), classOf[Boxed159[K]]),
      (f160[K](_), classOf[Boxed160[K]]),
      (f161[K](_), classOf[Boxed161[K]]),
      (f162[K](_), classOf[Boxed162[K]]),
      (f163[K](_), classOf[Boxed163[K]]),
      (f164[K](_), classOf[Boxed164[K]]),
      (f165[K](_), classOf[Boxed165[K]]),
      (f166[K](_), classOf[Boxed166[K]]),
      (f167[K](_), classOf[Boxed167[K]]),
      (f168[K](_), classOf[Boxed168[K]]),
      (f169[K](_), classOf[Boxed169[K]]),
      (f170[K](_), classOf[Boxed170[K]]),
      (f171[K](_), classOf[Boxed171[K]]),
      (f172[K](_), classOf[Boxed172[K]]),
      (f173[K](_), classOf[Boxed173[K]]),
      (f174[K](_), classOf[Boxed174[K]]),
      (f175[K](_), classOf[Boxed175[K]]),
      (f176[K](_), classOf[Boxed176[K]]),
      (f177[K](_), classOf[Boxed177[K]]),
      (f178[K](_), classOf[Boxed178[K]]),
      (f179[K](_), classOf[Boxed179[K]]),
      (f180[K](_), classOf[Boxed180[K]]),
      (f181[K](_), classOf[Boxed181[K]]),
      (f182[K](_), classOf[Boxed182[K]]),
      (f183[K](_), classOf[Boxed183[K]]),
      (f184[K](_), classOf[Boxed184[K]]),
      (f185[K](_), classOf[Boxed185[K]]),
      (f186[K](_), classOf[Boxed186[K]]),
      (f187[K](_), classOf[Boxed187[K]]),
      (f188[K](_), classOf[Boxed188[K]]),
      (f189[K](_), classOf[Boxed189[K]]),
      (f190[K](_), classOf[Boxed190[K]]),
      (f191[K](_), classOf[Boxed191[K]]),
      (f192[K](_), classOf[Boxed192[K]]),
      (f193[K](_), classOf[Boxed193[K]]),
      (f194[K](_), classOf[Boxed194[K]]),
      (f195[K](_), classOf[Boxed195[K]]),
      (f196[K](_), classOf[Boxed196[K]]),
      (f197[K](_), classOf[Boxed197[K]]),
      (f198[K](_), classOf[Boxed198[K]]),
      (f199[K](_), classOf[Boxed199[K]]),
      (f200[K](_), classOf[Boxed200[K]]),
      (f201[K](_), classOf[Boxed201[K]]),
      (f202[K](_), classOf[Boxed202[K]]),
      (f203[K](_), classOf[Boxed203[K]]),
      (f204[K](_), classOf[Boxed204[K]]),
      (f205[K](_), classOf[Boxed205[K]]),
      (f206[K](_), classOf[Boxed206[K]]),
      (f207[K](_), classOf[Boxed207[K]]),
      (f208[K](_), classOf[Boxed208[K]]),
      (f209[K](_), classOf[Boxed209[K]]),
      (f210[K](_), classOf[Boxed210[K]]),
      (f211[K](_), classOf[Boxed211[K]]),
      (f212[K](_), classOf[Boxed212[K]]),
      (f213[K](_), classOf[Boxed213[K]]),
      (f214[K](_), classOf[Boxed214[K]]),
      (f215[K](_), classOf[Boxed215[K]]),
      (f216[K](_), classOf[Boxed216[K]]),
      (f217[K](_), classOf[Boxed217[K]]),
      (f218[K](_), classOf[Boxed218[K]]),
      (f219[K](_), classOf[Boxed219[K]]),
      (f220[K](_), classOf[Boxed220[K]]),
      (f221[K](_), classOf[Boxed221[K]]),
      (f222[K](_), classOf[Boxed222[K]]),
      (f223[K](_), classOf[Boxed223[K]]),
      (f224[K](_), classOf[Boxed224[K]]),
      (f225[K](_), classOf[Boxed225[K]]),
      (f226[K](_), classOf[Boxed226[K]]),
      (f227[K](_), classOf[Boxed227[K]]),
      (f228[K](_), classOf[Boxed228[K]]),
      (f229[K](_), classOf[Boxed229[K]]),
      (f230[K](_), classOf[Boxed230[K]]),
      (f231[K](_), classOf[Boxed231[K]]),
      (f232[K](_), classOf[Boxed232[K]]),
      (f233[K](_), classOf[Boxed233[K]]),
      (f234[K](_), classOf[Boxed234[K]]),
      (f235[K](_), classOf[Boxed235[K]]),
      (f236[K](_), classOf[Boxed236[K]]),
      (f237[K](_), classOf[Boxed237[K]]),
      (f238[K](_), classOf[Boxed238[K]]),
      (f239[K](_), classOf[Boxed239[K]]),
      (f240[K](_), classOf[Boxed240[K]]),
      (f241[K](_), classOf[Boxed241[K]]),
      (f242[K](_), classOf[Boxed242[K]]),
      (f243[K](_), classOf[Boxed243[K]]),
      (f244[K](_), classOf[Boxed244[K]]),
      (f245[K](_), classOf[Boxed245[K]]),
      (f246[K](_), classOf[Boxed246[K]]),
      (f247[K](_), classOf[Boxed247[K]]),
      (f248[K](_), classOf[Boxed248[K]]),
      (f249[K](_), classOf[Boxed249[K]]),
      (f250[K](_), classOf[Boxed250[K]]))

  private[this] def boxes[K]: AtomicReference[List[(K => Boxed[K], Class[_ <: Boxed[K]])]] =
    new AtomicReference(allBoxes[K])

  def allClasses[K]: Seq[Class[_ <: Boxed[K]]] = allBoxes[K].map(_._2)

  // The `Class` type is invariant in its type parameter, which is why this
  // `_.isInstanceOf` is necessary and the `IsInstanceOf` warning is disabled.
  //
  // The `Throw` warning is suppressed because I (@Gabriel439) do not understand
  // the rationale behind the `Boxed` mechanism enough to say whether or not
  // this could be done in a more type-safe way.  However, I've added a more
  // helpful error message in the interim.
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.IsInstanceOf", "org.brianmckenna.wartremover.warts.Throw"))
  def next[K]: (K => Boxed[K], Class[Boxed[K]]) = boxes[K].get match {
    case list @ (h :: tail) if boxes.compareAndSet(list, tail) =>
      h.asInstanceOf[(K => Boxed[K], Class[Boxed[K]])]
    case (h :: tail) => next[K] // Try again
    case Nil => sys.error(
      """|Scalding's ordered serialization logic exhausted the finite supply of boxed classes.
           |
           |Resolution: Report this error to the Scalding maintainers
           |
           |Explanation: Scalding's ordered serialization logic internally uses
           |a large, but fixed, supply of unique wrapper types to box values in
           |order to control which serialization is used.  Exhausting this
           |supply is unusual and possibly indicative of another problem, unless           |you just happen to have a very complex Scalding job that uses
           |ordered serialization for a very large number of diverse types.  If
           |you legitimately need such a complex job then you will need to ask
           |the Scalding maintainers to increase the supply""".stripMargin)
  }
}
