package com.twitter.scalding.bdd

import org.scalatest.{ Matchers, WordSpec }
import com.twitter.scalding.RichPipe
import scala.collection.mutable.Buffer
import cascading.tuple.Tuple
import cascading.pipe.Pipe
import com.twitter.scalding.Dsl._

class SourceListSpecTest extends WordSpec with Matchers with BddDsl {

  "A test with a list of sources" should {
    "compile mixing it with a multi pipe function but fail if not same cardinality between given and when clause" in {
      an[IllegalArgumentException] should be thrownBy {
        Given {
          List(
            (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)),
            (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col3)),
            (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col4)))
        } When {
          (pipe1: RichPipe, pipe2: RichPipe) =>
            {
              pipe1
                .joinWithSmaller('col1 -> 'col1, pipe2)
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

    "work properly with a multi rich-pipe function with same cardinality" in {
      Given {
        List(
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col3)))
      } When {
        (pipe1: RichPipe, pipe2: RichPipe) =>
          {
            pipe1
              .joinWithSmaller('col1 -> 'col1, pipe2)
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

    "work properly with a multi pipe function with same cardinality" in {
      Given {
        List(
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col3)))
      } When {
        (pipe1: Pipe, pipe2: Pipe) =>
          {
            pipe1
              .joinWithSmaller('col1 -> 'col1, pipe2)
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

    "work properly with a function accepting a list of rich pipes" in {
      Given {
        List(
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col3)))
      } When {
        (pipes: List[RichPipe]) =>
          {
            pipes.head
              .joinWithSmaller('col1 -> 'col1, pipes(1))
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

    "work properly with a function accepting a list of pipes" in {
      Given {
        List(
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col2)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema ('col1, 'col3)))
      } When {
        (pipes: List[Pipe]) =>
          {
            pipes.head
              .joinWithSmaller('col1 -> 'col1, pipes(1))
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
