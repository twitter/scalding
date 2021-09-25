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
package com.twitter.scalding

import com.twitter.scalding.typed.CoGrouped.distinctBy

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object DistinctByProps extends Properties("CoGrouped.DistinctBy") {

  property("distinctBy never increases size") = forAll { (l: List[Int], fn: Int => Byte) =>
    distinctBy(l)(fn).size <= l.size
  }
  property("distinctBy.size == map(fn).toSet.size") = forAll { (l: List[Int], fn: Int => Byte) =>
    distinctBy(l)(fn).size == l.map(fn).toSet.size
  }
  property("distinctBy to unit gives size 0 or 1") = forAll { (l: List[Int], fn: Int => Unit) =>
    val dsize = distinctBy(l)(fn).size
    ((dsize == 0) && l.isEmpty) || dsize == 1
  }
  property("distinctBy to different values never changes the list") = forAll { (l: List[Int]) =>
    var idx = 0
    val fn = { (i: Int) => idx += 1; idx }
    distinctBy(l)(fn) == l
  }
  property("distinctBy works like groupBy(fn).map(_._2.head).toSet") = forAll { (l: List[Int], fn: Int => Byte) =>
    distinctBy(l)(fn).toSet == l.groupBy(fn).map(_._2.head).toSet
  }
  property("distinctBy matches a mutable implementation") = forAll { (l: List[Int], fn: Int => Byte) =>
    val dlist = distinctBy(l)(fn)
    var seen = Set[Byte]()
    l.flatMap { it =>
      if (seen(fn(it))) Nil
      else {
        seen += fn(it)
        List(it)
      }
    } == dlist
  }
}
