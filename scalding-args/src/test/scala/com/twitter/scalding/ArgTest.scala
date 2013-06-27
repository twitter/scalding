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
import org.specs._

class ArgTest extends Specification {
  "Tool.parseArgs" should {
    "handle the empty list" in {
      val map = Args(Array[String]())
      map.list("") must be_==(List())
    }
    "accept any number of dashed args" in {
      val map = Args(Array("--one", "1", "--two", "2", "--three", "3"))
      map.list("") must be_==(List())
      map.optional("") must be_==(None)

      map.list("absent") must be_==(List())
      map.optional("absent") must be_==(None)

      map("one") must be_==("1")
      map.list("one") must be_==(List("1"))
      map.required("one") must be_==("1")
      map.optional("one") must be_==(Some("1"))

      map("two") must be_==("2")
      map.list("two") must be_==(List("2"))
      map.required("two") must be_==("2")
      map.optional("two") must be_==(Some("2"))

      map("three") must be_==("3")
      map.list("three") must be_==(List("3"))
      map.required("three") must be_==("3")
      map.optional("three") must be_==(Some("3"))
    }
    "remove empty args in lists" in {
      val map = Args(Array("", "hello", "--one", "1", "", "\t", "--two", "2", "", "3"))
      map("") must be_==("hello")
      map.list("") must be_==(List("hello"))
      map("one") must be_==("1")
      map.list("one") must be_==(List("1"))
      map.list("two") must be_==(List("2", "3"))
    }
    "put initial args into the empty key" in {
      val map =Args(List("hello", "--one", "1"))
      map("") must be_==("hello")
      map.list("") must be_==(List("hello"))
      map.required("") must be_==("hello")
      map.optional("") must be_==(Some("hello"))

      map("one") must be_==("1")
      map.list("one") must be_==(List("1"))
    }
    "allow any number of args per key" in {
      val map = Args(Array("--one", "1", "--two", "2", "deux", "--zero"))
      map("one") must be_==("1")
      map.list("two") must be_==(List("2","deux"))
      map.boolean("zero") must be_==(true)
    }
    "allow any number of dashes" in {
      val map = Args(Array("-one", "1", "--two", "2", "---three", "3"))
      map("three") must be_==("3")
      map("two") must be_==("2")
      map("one") must be_==("1")
    }
    "round trip to/from string" in {
      val a = Args("--you all every --body 1 2")
      a must be_==(Args(a.toString))
      a must be_==(Args(a.toList))
    }
    "handle positional arguments" in {
      val a = Args("p0 p1 p2 --f 1 2")
      a.positional must be_==(List("p0", "p1", "p2"))
      Args(a.toString) must be_==(a)
      Args(a.toList) must be_==(a)
    }
    "handle negative numbers in args" in {
      val a = Args("--a 1 -2.1 --b 1 -3 4 --c -5")
      a.list("a") must_== List("1", "-2.1")
      a.list("b") must_== List("1", "-3", "4")
      a("c").toInt must_== -5
    }
    "handle k, v pairs separated by an equal sign" in {
      val a = Args("a=1")
      a("a") must be_==("1")
    }
    "handle multiple arguments when k, v pairs separated by an equal sign" in {
      val a = Args("a=1 2 3")
      a.list("a") must_== List("1", "2", "3")
    }
  }
}
