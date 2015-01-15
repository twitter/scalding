package com.twitter.scalding_internal.db.jdbc

import com.twitter.scalding._
import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.macros._

import org.scalatest.WordSpec

class TypedJDBCSourceJobTest extends WordSpec {
  class TestJob(args: Args) extends Job(args) {
    TypedPipe.from(ExampleTypedJDBCSource())
      .flatMap {
        case User(date_id, user_name, Some(age), gender) =>
          if (age % 2 == 0) Some((date_id, user_name, gender))
          else None
      }
      .write(TypedTsv[(Int, String, String)](args("output")))
  }

  class TestToIteratorJob(args: Args) extends Job(args) {
    TypedPipe.from(ExampleTypedJDBCSource().toIterator.toList)
      .flatMap {
        case User(date_id, user_name, Some(age), gender) =>
          if (age % 2 == 0) Some((date_id, user_name, gender))
          else None
      }
      .write(TypedTsv[(Int, String, String)](args("output")))
  }

  val data = List(
    User(121, "user1", Some(13), "male"),
    User(122, "user2", Some(14), "male"),
    User(123, "user3", None, "male"),
    User(124, "user4", Some(16), "female"))

  // TODO: JobTest currently fails if there are Options in case class (works on hadoop). fix it.
  /*
  "TypedJDBCSource" should {
    JobTest(new TestJob(_))
      .arg("output", "test_output")
      .source(ExampleTypedJDBCSource(), data)
      .sink[(Int, String, String)](TypedTsv[(Int, String, String)]("test_output")) { o =>
        o foreach println
      }
      .run
      .finish
  }
  */
}
