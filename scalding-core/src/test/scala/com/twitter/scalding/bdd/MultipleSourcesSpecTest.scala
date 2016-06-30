package com.twitter.scalding.bdd

import org.scalatest.{ Matchers, WordSpec }
import com.twitter.scalding.Dsl._
import com.twitter.scalding.RichPipe
import scala.collection.mutable.Buffer
import cascading.tuple.Tuple

class MultipleSourcesSpecTest extends WordSpec with Matchers with BddDsl {

  "A test with two sources" should {
    "accept an operation with two input pipes" in {
      Given {
        List(("Stefano", "110"), ("Rajah", "220")) withSchema ('name, 'points)
      } And {
        List(("Stefano", "home1"), ("Rajah", "home2")) withSchema ('name, 'address)
      } When {
        (pipe1: RichPipe, pipe2: RichPipe) =>
          {
            pipe1.joinWithSmaller('name -> 'name, pipe2).map('address -> 'address_transf) {
              address: String => address + "_transf"
            }
          }
      } Then {
        buffer: Buffer[(String, String, String, String)] =>
          {
            buffer.forall({
              case (_, _, _, addressTransf) => addressTransf.endsWith("_transf")
            }) shouldBe true
          }
      }
    }

    "accept an operation with two input pipes using Tuples" in {
      Given {
        List(new Tuple("Stefano", "110"), new Tuple("Rajah", "220")) withSchema ('name, 'points)
      } And {
        List(new Tuple("Stefano", "home1"), new Tuple("Rajah", "home2")) withSchema ('name, 'address)
      } When {
        (pipe1: RichPipe, pipe2: RichPipe) =>
          {
            pipe1.joinWithSmaller('name -> 'name, pipe2).map('address -> 'address_transf) {
              address: String => address + "_transf"
            }
          }
      } Then {
        buffer: Buffer[(String, String, String, String)] =>
          {
            buffer.forall({
              case (_, _, _, addressTransf) => addressTransf.endsWith("_transf")
            }) shouldBe true
          }
      }
    }
  }

  "A test with three sources" should {
    "accept an operation with three input pipes" in {
      Given {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col3)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col4)
      } When {
        (pipe1: RichPipe, pipe2: RichPipe, pipe3: RichPipe) =>
          {
            pipe1
              .joinWithSmaller('col1 -> 'col1, pipe2)
              .joinWithSmaller('col1 -> 'col1, pipe3)
              .map('col1 -> 'col1_transf) {
                col1: String => col1 + "_transf"
              }
              .project(('col1, 'col2, 'col1_transf))
          }
      } Then {
        buffer: Buffer[Tuple] =>
          {
            buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) shouldBe true
          }
      }
    }
  }

  "A test with four sources" should {
    "compile mixing an operation with inconsistent number of input pipes but fail at runtime" in {
      an[IllegalArgumentException] should be thrownBy {
        Given {
          List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)
        } And {
          List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col3)
        } And {
          List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col4)
        } And {
          List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col5)
        } When {
          (pipe1: RichPipe, pipe2: RichPipe, pipe3: RichPipe) =>
            {
              pipe1
                .joinWithSmaller('col1 -> 'col1, pipe2)
                .joinWithSmaller('col1 -> 'col1, pipe3)
                .joinWithSmaller('col1 -> 'col1, pipe3)
                .map('col1 -> 'col1_transf) {
                  col1: String => col1 + "_transf"
                }
            }
        } Then {
          buffer: Buffer[Tuple] =>
            {
              buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) shouldBe true
            }
        }
      }
    }

    "be used with a function accepting a list of sources because there is no implicit for functions with more than three input pipes" in {
      Given {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col4)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col5)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col6)
      } When {
        (pipes: List[RichPipe]) =>
          {
            pipes.head
              .joinWithSmaller('col1 -> 'col1, pipes(1))
              .joinWithSmaller('col1 -> 'col1, pipes(2))
              .joinWithSmaller('col1 -> 'col1, pipes(3))
              .map('col1 -> 'col1_transf) {
                col1: String => col1 + "_transf"
              }
              .project(('col1, 'col2, 'col1_transf))
          }
      } Then {
        buffer: Buffer[Tuple] =>
          {
            buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) shouldBe true
          }
      }
    }
  }
}
