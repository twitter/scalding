package com.twitter.scalding.quotation

import org.scalatest.MustMatchers
import org.scalatest.FreeSpec

trait S

trait T1 extends S
trait T2

trait P1 extends S
trait P2

class ProjectionTest extends Test {

  val t1 = TypeReference(typeName[T1])
  val p1 = Property(t1, Accessor("p1"), typeName[P1])

  val t2 = TypeReference(TypeName(classOf[T2].getName))
  val p2 = Property(t2, Accessor("p2"), typeName[P2])

  "Projection" - {
    "andThen" - {
      "TypeReference" in {
        t1.andThen(p1.accessor, p1.typeName) mustEqual p1
      }
      "Property" in {
        p1.andThen(Accessor("p2"), TypeName("p2t")) mustEqual
          Property(p1, Accessor("p2"), TypeName("p2t"))
      }
    }

    "toString" - {
      "TypeReference" - {
        "simple" in {
          t1.toString mustEqual "T1"
        }
        "ignores package" in {
          TypeReference(TypeName("com.twitter.Test1")).toString mustEqual "Test1"
        }
      }
      "Property" in {
        p1.toString() mustEqual "T1.p1"
      }
    }
  }

  "Projections" - {
    "empty" in {
      Projections.empty.set mustEqual Set()
    }
    "apply" - {
      "simple" in {
        val set = Set[Projection](p1)
        Projections(set).set mustEqual set
      }
      "paths merge" - {
        "simple" in {
          val set = Set[Projection](p1, t1)
          Projections(set).set mustEqual Set(t1)
        }
        "nested" in {
          val px = p1.andThen(Accessor("x"), TypeName("X"))
          val set = Set[Projection](px, t1)
          Projections(set).set mustEqual Set(t1)
        }
      }
    }
    "flatten" - {
      "empty" in {
        Projections.flatten(Nil).set mustEqual Set()
      }
      "non-empty" in {
        val list = List(
          Projections(Set(p1)),
          Projections(Set(p2)))
        Projections.flatten(list).set mustEqual Set(p1, p2)
      }
      "non-empty with merge" in {
        val list = List(
          Projections(Set(t1)),
          Projections(Set(p1)))
        Projections.flatten(list).set mustEqual Set(t1)
      }
    }

    "++" - {
      "simple" in {
        val p = Projections(Set(p1)) ++ Projections(Set(p2))
        p.set mustEqual Set(p1, p2)
      }
      "with merge" in {
        val list = List(
          Projections(Set(p1)),
          Projections(Set(t1)))
        Projections.flatten(list).set mustEqual Set(t1)
      }
    }

    "toString" - {
      "empty" in {
        Projections.empty.toString mustEqual "Projections()"
      }
      "non-empty" in {
        Projections(Set(p1, p2)).toString mustEqual "Projections(T1.p1, T2.p2)"
      }
    }

    "basedOn" - {
      "empty base" in {
        Projections(Set(p1, p2)).basedOn(Set.empty) mustEqual
          Projections.empty
      }
      "no match" in {
        Projections(Set(p1, p2)).basedOn(Set(TypeReference(TypeName("X")))) mustEqual
          Projections.empty
      }
      "one match" in {
        val px1 = Property(TypeReference(TypeName("X")), Accessor("px"), typeName[T1])
        Projections(Set(p1, p2)).basedOn(Set(px1)).set mustEqual
          Set(p1.copy(path = px1))
      }
      "multiple matches" in {
        val px1 = Property(TypeReference(TypeName("X1")), Accessor("px1"), typeName[T1])
        val px2 = Property(TypeReference(TypeName("X1")), Accessor("px2"), typeName[T2])
        Projections(Set(p1, p2)).basedOn(Set(px1, px2)).set mustEqual
          Set(p1.copy(path = px1), p2.copy(path = px2))
      }
      "partial match" in {
        val px1 = Property(TypeReference(TypeName("X1")), Accessor("px1"), typeName[T1])
        val px2 = Property(TypeReference(TypeName("X1")), Accessor("px2"), TypeName("TX"))
        Projections(Set(p1, p2)).basedOn(Set(px1, px2)).set mustEqual
          Set(p1.copy(path = px1))
      }
    }

    "of" - {
      "byType" - {
        "matches" in {
          Projections(Set(t1)).of(t1.typeName, classOf[Any]).set mustEqual
            Set(t1)
        }
        "doesn't match" in {
          Projections(Set(t1)).of(TypeName("X"), classOf[Any]).set mustEqual
            Set.empty
        }
        "nested" in {
          val px = Property(p1, Accessor("px"), TypeName("PX"))
          Projections(Set(px)).of(typeName[T1], classOf[Any]).set mustEqual
            Set(px)
        }
      }
      "bySuperClass" - {
        "filters only projections of the super class type" in {
          val px = p1.andThen(Accessor("px"), typeName[String])
          val py = px.andThen(Accessor("isEmpty"), typeName[Boolean])
          Projections(Set(py)).of(t1.typeName, classOf[S]).set mustEqual Set(px)
        }
        "ignores if class can't be loaded" in {
          val tx = TypeReference(TypeName("TX"))
          Projections(Set(tx)).of(tx.typeName, classOf[Any]).set mustEqual
            Set.empty
        }
      }
    }

  }
}