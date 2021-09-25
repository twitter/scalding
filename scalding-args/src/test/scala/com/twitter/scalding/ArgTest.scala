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
import org.scalatest.WordSpec

class ArgTest extends WordSpec {
  "Tool.parseArgs" should {

    "handle the empty list" in {
      val map = Args(Array[String]())
      assert(map.list("").isEmpty)
    }

    "accept any number of dashed args" in {
      val map = Args(Array("--one", "1", "--two", "2", "--three", "3"))
      assert(map.list("").isEmpty)
      assert(map.optional("").isEmpty)

      assert(map.list("absent").isEmpty)
      assert(map.optional("absent").isEmpty)

      assert(map("one") === "1")
      assert(map.list("one") === List("1"))
      assert(map.required("one") === "1")
      assert(map.optional("one") === Some("1"))

      assert(map("two") === "2")
      assert(map.list("two") === List("2"))
      assert(map.required("two") === "2")
      assert(map.optional("two") === Some("2"))

      assert(map("three") === "3")
      assert(map.list("three") === List("3"))
      assert(map.required("three") === "3")
      assert(map.optional("three") === Some("3"))
    }

    "remove empty args in lists" in {
      val map = Args(Array("", "hello", "--one", "1", "", "\t", "--two", "2", "", "3"))
      assert(map("") === "hello")
      assert(map.list("") === List("hello"))
      assert(map("one") === "1")
      assert(map.list("one") === List("1"))
      assert(map.list("two") === List("2", "3"))
    }

    "put initial args into the empty key" in {
      val map = Args(List("hello", "--one", "1"))
      assert(map("") === "hello")
      assert(map.list("") === List("hello"))
      assert(map.required("") === "hello")
      assert(map.optional("") === Some("hello"))

      assert(map("one") === "1")
      assert(map.list("one") === List("1"))
    }

    "allow any number of args per key" in {
      val map = Args(Array("--one", "1", "--two", "2", "deux", "--zero"))
      assert(map("one") === "1")
      assert(map.list("two") === List("2", "deux"))
      assert(map.boolean("zero"))
    }

    "allow any number of dashes" in {
      val map = Args(Array("-one", "1", "--two", "2", "---three", "3"))
      assert(map("three") === "3")
      assert(map("two") === "2")
      assert(map("one") === "1")
    }

    "round trip to/from string" in {
      val a = Args("--you all every --body 1 2")
      assert(a === Args(a.toString))
      assert(a === Args(a.toList))
    }

    "handle positional arguments" in {
      val a = Args("p0 p1 p2 --f 1 2")
      assert(a.positional === List("p0", "p1", "p2"))
      assert(Args(a.toString) === a)
      assert(Args(a.toList) === a)
    }

    "handle negative numbers in args" in {
      val a = Args("--a 1 -2.1 --b 1 -3 4 --c -5")
      assert(a.list("a") === List("1", "-2.1"))
      assert(a.list("b") === List("1", "-3", "4"))
      assert(a("c").toInt === -5)
    }

    "handle strange characters in the args" in {
      val a = Args("p-p --a-a 1-1 -b=b c=d e/f -5,2 5,3")
      assert(a.positional === List("p-p"))
      assert(a.list("a-a") === List("1-1"))
      assert(a.list("b=b") === List("c=d", "e/f"))
      assert(a.list("5,2") === List("5,3"))
    }

    "access positional arguments using apply" in {
      val a = Args("a b c --d e")
      assert(a(0) === "a")
      assert(a(1) === "b")
      assert(a(2) === "c")
      assert(a("d") === "e")
    }

    "verify that args belong to an accepted key set" in {
      val a = Args("a --one --two b --three c d --scalding.tool.mode")
      a.restrictTo(Set("one", "two", "three", "four"))
      intercept[RuntimeException] { a.restrictTo(Set("one", "two")) }
    }

    "correctly parse numeric args" in {
      val map = Args(Array("--anInt", "-1", "--aLong", "21474836470", "--aDecimal", "3.141592654", "--aString", "foo"))
      assert(map.int("anInt") == "-1".toInt)
      assert(map.int("anInt", 2) == "-1".toInt)
      assert(map.int("nothing", 2) == 2)
      intercept[RuntimeException] { map.int("nothing") }
      intercept[RuntimeException] { map.int("aString") }
      intercept[RuntimeException] { map.int("aString", 2) }

      assert(map.long("aLong") == "21474836470".toLong)
      assert(map.long("anInt", 2L) == "-1".toLong)
      assert(map.long("nothing", 2L) == 2L)
      intercept[RuntimeException] { map.long("nothing") }
      intercept[RuntimeException] { map.long("aString") }
      intercept[RuntimeException] { map.long("aString", 2L) }

      assert(map.float("aDecimal") == "3.141592654".toFloat)
      assert(map.float("aDecimal", 2.71828f) == "3.141592654".toFloat)
      assert(map.float("nothing", 2.71828f) == 2.71828f)
      intercept[RuntimeException] { map.float("nothing") }
      intercept[RuntimeException] { map.float("aString") }
      intercept[RuntimeException] { map.float("aString", 2.71828f) }

      assert(map.double("aDecimal") == "3.141592654".toDouble)
      assert(map.double("aDecimal", 2.71828d) == "3.141592654".toDouble)
      assert(map.double("nothing", 2.71828d) == 2.71828d)
      intercept[RuntimeException] { map.double("nothing") }
      intercept[RuntimeException] { map.double("aString") }
      intercept[RuntimeException] { map.double("aString", 2.71828d) }
    }
  }
}
