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

import cascading.tuple.Fields

import org.scalatest.{ Matchers, WordSpec }

class FieldImpsTest extends WordSpec with Matchers with FieldConversions {
  def setAndCheck[T <: Comparable[_]](v: T)(implicit conv: (T) => Fields): Unit = {
    conv(v) shouldBe (new Fields(v))
  }
  def setAndCheckS[T <: Comparable[_]](v: Seq[T])(implicit conv: (Seq[T]) => Fields): Unit = {
    conv(v) shouldBe (new Fields(v: _*))
  }
  def setAndCheckSym(v: Symbol): Unit = {
    (v: Fields) shouldBe (new Fields(v.toString.tail))
  }
  def setAndCheckSymS(v: Seq[Symbol]): Unit = {
    (v: Fields) shouldBe (new Fields(v.map(_.toString.tail): _*))
  }
  def setAndCheckField(v: Field[_]): Unit = {
    val vF: Fields = v
    val fields = new Fields(v.id)
    fields.setComparators(v.ord)
    checkFieldsWithComparators(vF, fields)
  }
  def setAndCheckFieldS(v: Seq[Field[_]]): Unit = {
    val vF: Fields = v
    val fields = new Fields(v.map(_.id): _*)
    fields.setComparators(v.map(_.ord): _*)
    checkFieldsWithComparators(vF, fields)
  }
  def setAndCheckEnumValue(v: Enumeration#Value): Unit = {
    (v: Fields) shouldBe (new Fields(v.toString))
  }
  def setAndCheckEnumValueS(v: Seq[Enumeration#Value]): Unit = {
    (v: Fields) shouldBe (new Fields(v.map(_.toString): _*))
  }
  def checkFieldsWithComparators(actual: Fields, expected: Fields): Unit = {
    // sometimes one or the other is actually a RichFields, so rather than test for
    // actual.equals(expected), we just check that all the field names and comparators line up
    actual should have size (expected.size)
    asList(actual) shouldBe asList(expected)
    actual.getComparators.toSeq shouldBe (expected.getComparators.toSeq)
  }
  "Field" should {
    "contain manifest" in {
      val field = Field[Long]("foo")
      field.mf should contain (implicitly[Manifest[Long]])
    }
  }
  "RichFields" should {
    "convert to Fields" in {
      val f1 = Field[Long]('foo)
      val f2 = Field[String]('bar)
      val rf = RichFields(f1, f2)
      val fields: Fields = rf
      fields should have size 2
      f1.id shouldBe (fields.get(0))
      f2.id shouldBe (fields.get(1))
      f1.ord shouldBe (fields.getComparators()(0))
      f2.ord shouldBe (fields.getComparators()(1))
    }
    "convert from Fields" in {
      val fields = new Fields("foo", "bar")
      val comparator = implicitly[Ordering[String]]
      fields.setComparators(comparator, comparator)
      val fieldList: List[Field[_]] = fields.toFieldList
      fieldList shouldBe List(new StringField[String]("foo")(comparator, None), new StringField[String]("bar")(comparator, None))
    }
    "throw an exception on when converting a virtual Fields instance" in {

      import Fields._
      List(ALL, ARGS, FIRST, GROUP, LAST, NONE, REPLACE, RESULTS, SWAP, UNKNOWN, VALUES).foreach { fields =>
        an[Exception] should be thrownBy fields.toFieldList
      }
    }
  }
  "Fields conversions" should {
    "convert from ints" in {
      setAndCheck(int2Integer(0))
      setAndCheck(int2Integer(5))
      setAndCheckS(List(1, 23, 3, 4).map(int2Integer))
      setAndCheckS((0 until 10).map(int2Integer))
    }
    "convert from strings" in {
      setAndCheck("hey")
      setAndCheck("world")
      setAndCheckS(List("one", "two", "three"))
      //Synonym for list
      setAndCheckS(Seq("one", "two", "three"))
    }
    "convert from symbols" in {
      setAndCheckSym('hey)
      //Shortest length to make sure the tail stuff is working:
      setAndCheckSym('h)
      setAndCheckSymS(List('hey, 'world, 'symbols))
    }
    "convert from com.twitter.scalding.Field instances" in {
      // BigInteger is just a convenient non-primitive ordered type
      setAndCheckField(Field[java.math.BigInteger]("foo"))
      setAndCheckField(Field[java.math.BigInteger]('bar))
      setAndCheckField(Field[java.math.BigInteger](0))
      // Try a custom ordering
      val ord = implicitly[Ordering[java.math.BigInteger]].reverse
      setAndCheckField(Field[java.math.BigInteger]("bell")(ord, implicitly[Manifest[java.math.BigInteger]]))
      setAndCheckFieldS(List(Field[java.math.BigInteger](0), Field[java.math.BigDecimal]("bar")))
    }
    "convert from enumeration values" in {
      object Schema extends Enumeration {
        val one, two, three = Value
      }
      import Schema._
      setAndCheckEnumValue(one)
      setAndCheckEnumValueS(List(one, two))
      setAndCheckEnumValueS(Seq(one, two, three))
    }
    "convert from enumerations" in {
      object Schema extends Enumeration {
        val one, two, three = Value
      }
      (Schema: Fields) shouldBe (new Fields("one", "two", "three"))
    }
    "convert from general int tuples" in {
      var vf: Fields = Tuple1(1)
      vf shouldBe (new Fields(int2Integer(1)))
      vf = (1, 2)
      vf shouldBe (new Fields(int2Integer(1), int2Integer(2)))
      vf = (1, 2, 3)
      vf shouldBe (new Fields(int2Integer(1), int2Integer(2), int2Integer(3)))
      vf = (1, 2, 3, 4)
      vf shouldBe (new Fields(int2Integer(1), int2Integer(2), int2Integer(3), int2Integer(4)))
    }
    "convert from general string tuples" in {
      var vf: Fields = Tuple1("hey")
      vf shouldBe (new Fields("hey"))
      vf = ("hey", "world")
      vf shouldBe (new Fields("hey", "world"))
      vf = ("foo", "bar", "baz")
      vf shouldBe (new Fields("foo", "bar", "baz"))
    }
    "convert from general symbol tuples" in {
      var vf: Fields = Tuple1('hey)
      vf shouldBe (new Fields("hey"))
      vf = ('hey, 'world)
      vf shouldBe (new Fields("hey", "world"))
      vf = ('foo, 'bar, 'baz)
      vf shouldBe (new Fields("foo", "bar", "baz"))
    }
    "convert from general com.twitter.scalding.Field tuples" in {
      val foo = Field[java.math.BigInteger]("foo")
      val bar = Field[java.math.BigDecimal]("bar")

      var vf: Fields = Tuple1(foo)
      var fields = new Fields("foo")
      fields.setComparators(foo.ord)
      checkFieldsWithComparators(vf, fields)

      vf = Tuple2(foo, bar)
      fields = new Fields("foo", "bar")
      fields.setComparators(foo.ord, bar.ord)
      checkFieldsWithComparators(vf, fields)

      vf = Tuple3(foo, bar, 'bell)
      fields = new Fields("foo", "bar", "bell")
      fields.setComparator("foo", foo.ord)
      fields.setComparator("bar", bar.ord)
      checkFieldsWithComparators(vf, fields)
    }
    "convert from general enumeration value tuples" in {
      object Schema extends Enumeration {
        val one, two, three = Value
      }
      import Schema._
      var vf: Fields = Tuple1(one)
      vf shouldBe (new Fields("one"))
      vf = (one, two)
      vf shouldBe (new Fields("one", "two"))
      vf = (one, two, three)
      vf shouldBe (new Fields("one", "two", "three"))
    }
    "convert to a pair of Fields from a pair of values" in {
      var f2: (Fields, Fields) = "hey" -> "you"
      f2 shouldBe (new Fields("hey"), new Fields("you"))

      f2 = 'hey -> 'you
      f2 shouldBe (new Fields("hey"), new Fields("you"))

      f2 = (0 until 10) -> 'you
      f2 shouldBe (new Fields((0 until 10).map(int2Integer): _*), new Fields("you"))

      f2 = (('hey, 'world) -> 'other)
      f2 shouldBe (new Fields("hey", "world"), new Fields("other"))

      f2 = 0 -> 2
      f2 shouldBe (new Fields(int2Integer(0)), new Fields(int2Integer(2)))

      f2 = (0, (1, "you"))
      f2 shouldBe (new Fields(int2Integer(0)), new Fields(int2Integer(1), "you"))

      val foo = Field[java.math.BigInteger]("foo")
      val bar = Field[java.math.BigDecimal]("bar")
      f2 = ((foo, bar) -> 'bell)
      var fields = new Fields("foo", "bar")
      fields.setComparators(foo.ord, bar.ord)
      f2 shouldBe (fields, new Fields("bell"))

      f2 = (foo -> ('bar, 'bell))
      fields = RichFields(foo)
      fields.setComparators(foo.ord)
      f2 shouldBe (fields, new Fields("bar", "bell"))

      f2 = Seq("one", "two", "three") -> Seq("1", "2", "3")
      f2 shouldBe (new Fields("one", "two", "three"), new Fields("1", "2", "3"))
      f2 = List("one", "two", "three") -> List("1", "2", "3")
      f2 shouldBe (new Fields("one", "two", "three"), new Fields("1", "2", "3"))
      f2 = List('one, 'two, 'three) -> List('n1, 'n2, 'n3)
      f2 shouldBe (new Fields("one", "two", "three"), new Fields("n1", "n2", "n3"))
      f2 = List(4, 5, 6) -> List(1, 2, 3)
      f2 shouldBe (new Fields(int2Integer(4), int2Integer(5), int2Integer(6)),
        new Fields(int2Integer(1), int2Integer(2), int2Integer(3)))

      object Schema extends Enumeration {
        val one, two, three = Value
      }
      import Schema._

      f2 = one -> two
      f2 shouldBe (new Fields("one"), new Fields("two"))

      f2 = (one, two) -> three
      f2 shouldBe (new Fields("one", "two"), new Fields("three"))

      f2 = one -> (two, three)
      f2 shouldBe (new Fields("one"), new Fields("two", "three"))
    }
    "correctly see if there are ints" in {
      hasInts(0) shouldBe true
      hasInts((0, 1)) shouldBe true
      hasInts('hey) shouldBe false
      hasInts((0, 'hey)) shouldBe true
      hasInts(('hey, 9)) shouldBe true
      hasInts(('a, 'b)) shouldBe false
      def i(xi: Int) = new java.lang.Integer(xi)
      asSet(0) shouldBe Set(i(0))
      asSet((0, 1, 2)) shouldBe Set(i(0), i(1), i(2))
      asSet((0, 1, 'hey)) shouldBe Set(i(0), i(1), "hey")
    }
    "correctly determine default modes" in {
      //Default case:
      defaultMode(0, 'hey) shouldBe Fields.ALL
      defaultMode((0, 't), 'x) shouldBe Fields.ALL
      defaultMode(('hey, 'x), 'y) shouldBe Fields.ALL
      //Equal:
      defaultMode('hey, 'hey) shouldBe Fields.REPLACE
      defaultMode(('hey, 'x), ('hey, 'x)) shouldBe Fields.REPLACE
      defaultMode(0, 0) shouldBe Fields.REPLACE
      //Subset/superset:
      defaultMode(('hey, 'x), 'x) shouldBe Fields.SWAP
      defaultMode('x, ('hey, 'x)) shouldBe Fields.SWAP
      defaultMode(0, ('hey, 0)) shouldBe Fields.SWAP
      defaultMode(('hey, 0), 0) shouldBe Fields.SWAP
    }

  }
}
