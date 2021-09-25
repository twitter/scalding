package com.twitter.scalding.quotation

class ProjectionMacroTest extends Test {

  class TestClass {
    def function[T, U](f: T => U)(implicit m: Quoted) = (m, f)
    def noProjection(i: Int)(implicit m: Quoted) = (m, i)
  }

  val test = new TestClass

  "no projection" in {
    test.noProjection(42)._1.projections.set mustEqual Set.empty
  }

  "method with params isn't considered as projection" in {
    test
      .function[Person, String](_.name.substring(1))._1
      .projections.set mustEqual Set(Person.nameProjection)
  }

  "simple" in {
    test.function[Person, String](_.name)._1
      .projections.set mustEqual Set(Person.nameProjection)
  }

  "nested" in {
    test.function[Person, String](_.contact.phone)._1
      .projections.set mustEqual Set(Person.phoneProjection)
  }

  "all properties" in {
    test.function[Person, Person](p => p)._1
      .projections.set mustEqual Set(Person.typeReference)
  }

  "empty projection" in {
    test.function[Person, Int](p => 1)._1
      .projections.set mustEqual Set.empty
  }

  "function call" - {
    "implicit apply" - {
      "non-quoted" in {
        val function = (p: Person) => p.name
        test.function[Person, String](p => function(p))._1
          .projections.set mustEqual Set(Person.typeReference)
      }
      "quoted" in {
        val function = Quoted.function {
          (p: Person) => p.name
        }
        test.function[Person, String](p => function(p))._1
          .projections.set mustEqual Set(Person.nameProjection)
      }
    }
    "explicit apply" - {
      "non-quoted" in {
        val function = (p: Person) => p.name
        test.function[Person, String](p => function.apply(p))._1
          .projections.set mustEqual Set(Person.typeReference)
      }
      "quoted" in {
        val function = Quoted.function {
          (p: Person) => p.name
        }
        test.function[Person, String](p => function.apply(p))._1
          .projections.set mustEqual Set(Person.nameProjection)
      }
    }
  }

  "function instance" - {
    "non-quoted" in {
      val function = (p: Person) => p.name
      test.function[Person, String](function)._1
        .projections.set mustEqual Set(Person.typeReference)
    }
    "quoted" in {
      val function = Quoted.function {
        (p: Person) => p.name
      }
      test.function[Person, String](function)._1
        .projections.set mustEqual Set(Person.nameProjection)
    }
  }

  "method call" - {
    "in the function body" in {
      def method(p: Person) = p.name
      test.function[Person, String](p => method(p))._1
        .projections.set mustEqual Set(Person.typeReference)
    }
    "as function" in {
      def method(p: Person) = p.name
      test.function[Person, String](method)._1
        .projections.set mustEqual Set(Person.typeReference)
    }
  }
}
