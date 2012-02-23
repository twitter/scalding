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

package com.twitter.scalding;

import cascading.tuple.Hasher;

import java.io.Serializable;
import java.util.Comparator;

/*
 * Handles numerical hashing properly
 */
class IntegralComparator extends Comparator[AnyRef] with Hasher[AnyRef] with Serializable {

  def isIntegral(boxed : AnyRef) = {
    val bclass = boxed.getClass
    if (bclass == classOf[java.lang.Long]) {
      true
    }
    else if (bclass == classOf[java.lang.Integer]) {
      true
    }
    else if (bclass == classOf[java.lang.Short]) {
      true
    }
    else if (bclass == classOf[java.lang.Byte]) {
      true
    }
    else {
      false
    }
  }

  override def compare(a1: AnyRef, a2: AnyRef) : Int = {
    if (isIntegral(a1) && isIntegral(a2)) {
      val long1 = a1.asInstanceOf[Number].longValue
      val long2 = a2.asInstanceOf[Number].longValue
      if (long1 < long2)
        -1
      else if (long1 > long2)
        1
      else
        0
    }
    else
      a1.asInstanceOf[Comparable[AnyRef]].compareTo(a2)
  }

  override def hashCode(obj : AnyRef) : Int = {
    if (isIntegral(obj)) {
      val longv = obj.asInstanceOf[Number].longValue
      (longv ^ (longv >>> 32)).toInt
    }
    else {
      //Use the default:
      obj.hashCode
    }
  }
}
