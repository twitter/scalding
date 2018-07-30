package com.twitter.scalding.bdd

import org.scalatest.{ Matchers, WordSpec }
import com.twitter.scalding.RichPipe
import scala.collection.mutable.Buffer
import cascading.pipe.Pipe
import cascading.tuple.Tuple
import com.twitter.scalding.Dsl._

class SingleSourceSpecTest extends WordSpec with Matchers with BddDsl {
  "A test with single source" should {
    "accept an operation with a single input rich pipe" in {
      Given {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
      } When {
        pipe: RichPipe =>
          {
            pipe.map('col1 -> 'col1_transf) {
              col1: String => col1 + "_transf"
            }
          }
      } Then {
        buffer: Buffer[(String, String, String)] =>
          {
            buffer.forall({
              case (_, _, transformed) => transformed.endsWith("_transf")
            }) shouldBe true
          }
      }
    }

    "accept an operation with a single input pipe" in {
      Given {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
      } When {
        pipe: Pipe =>
          {
            pipe.map('col1 -> 'col1_transf) {
              col1: String => col1 + "_transf"
            }
          }
      } Then {
        buffer: Buffer[(String, String, String)] =>
          {
            buffer.forall({
              case (_, _, transformed) => transformed.endsWith("_transf")
            }) shouldBe true
          }
      }
    }

    "work with output as Tuple" in {
      Given {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
      } When {
        pipe: RichPipe =>
          {
            pipe.map('col1 -> 'col1_transf) {
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

    "work with input as simple type" in {
      Given {
        List("col1_1", "col1_2") withSchema ('col1)
      } When {
        pipe: RichPipe =>
          {
            pipe.map('col1 -> 'col1_transf) {
              col1: String => col1 + "_transf"
            }
          }
      } Then {
        buffer: Buffer[Tuple] =>
          {
            buffer.forall(tuple => tuple.getString(1).endsWith("_transf")) shouldBe true
          }
      }
    }

    "work with input as Tuple" in {
      Given {
        List(new Tuple("col1_1", "col2_1"), new Tuple("col1_2", "col2_2")) withSchema (('col1, 'col2))
      } When {
        pipe: RichPipe =>
          {
            pipe.map('col1 -> 'col1_transf) {
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
}
