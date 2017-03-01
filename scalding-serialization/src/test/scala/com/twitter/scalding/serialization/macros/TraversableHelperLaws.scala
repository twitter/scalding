/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.scalding.serialization.macros

import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.prop.Checkers
import org.scalatest.prop.PropertyChecks

import impl.ordered_serialization.runtime_helpers.TraversableHelpers._

class TraversableHelperLaws extends FunSuite with PropertyChecks with Matchers {
  test("Iterator ordering should be Iterable ordering") {
    forAll { (l1: List[Int], l2: List[Int]) =>
      assert(iteratorCompare[Int](l1.iterator, l2.iterator) ===
        Ordering[Iterable[Int]].compare(l1, l2), "Matches scala's Iterable compare")
    }
  }
  test("Iterator equiv should be Iterable ordering") {
    forAll { (l1: List[Int], l2: List[Int]) =>
      assert(iteratorEquiv[Int](l1.iterator, l2.iterator) ===
        Ordering[Iterable[Int]].equiv(l1, l2), "Matches scala's Iterable compare")
    }
  }
  test("sortedCompare matches sort followed by compare List[Int]") {
    forAll(minSuccessful(1000)) { (l1: List[Int], l2: List[Int]) =>
      assert(sortedCompare[Int](l1, l2) ===
        Ordering[Iterable[Int]].compare(l1.sorted, l2.sorted), "Matches scala's Iterable compare")
    }
  }
  test("sortedCompare matches sort followed by compare Set[Int]") {
    forAll(minSuccessful(1000)) { (l1: Set[Int], l2: Set[Int]) =>
      assert(sortedCompare[Int](l1, l2) ===
        Ordering[Iterable[Int]].compare(l1.toList.sorted, l2.toList.sorted), "Matches scala's Iterable compare")
    }
  }
}
