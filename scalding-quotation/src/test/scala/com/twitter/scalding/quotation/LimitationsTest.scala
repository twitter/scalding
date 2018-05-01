package com.twitter.scalding.quotation

class LimitationsTest extends Test {

  class TestClass {
    def function[T, U](f: T => U)(implicit q: Quoted) = (q, f)
  }

  val test = new TestClass

  "nested transitive projection" in pendingUntilFixed {
    test.function[Person, Option[String]](_.alternativeContact.map(_.phone))._1.projections.set mustEqual
      Set(Person.typeReference.andThen(Accessor("alternativeContact"), typeName[Option[Contact]]).andThen(Accessor("phone"), typeName[String]))
  }

  "nested quoted function projection" in pendingUntilFixed {
    val contactFunction = Quoted.function {
      (p: Person) => p.contact
    }
    val phoneFunction = Quoted.function {
      (p: Person) => contactFunction(p).phone
    }
    phoneFunction.quoted.projections.set mustEqual Set(Person.phoneProjection)
  }
}