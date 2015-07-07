/*
Copyright 2015 Twitter, Inc.

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

package com.twitter.scalding.db

import com.twitter.scalding._
import com.twitter.scalding.db.macros._

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
