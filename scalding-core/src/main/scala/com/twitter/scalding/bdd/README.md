# BDD DSL to Test Pipe Transforming Functions Outside of a Job

The idea of this package is to allow user to write Scalding jobs in a more modular and test-driven way.
It essentially provides a test harness to support the decomposition of a Scalding Map-Reduce Job into a series of smaller steps,
each one testable independently before being assembled into the main Job that will then be tested as a whole using Scalding-based
tests.
It is not an alternative to the JobTest class but it covers a different part of the testing phase. The JobTest class can be used
to test a full Job end to end while this framework allows you to test every single sub-step in isolation.
It supports fields API using the BddDsl package and typed API using TBddDsl

## What does it look like

A test written with using the BddDsl or the TBddDsl look as shown below:

With Specs

```scala
class SingleSourceSpecTest extends Specification with BddDsl {
  "A test with single source" should {
    "add user info" in {
        Given {
          List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
        } And {
          List( (1000002l, "stefano@email.com", "10 Downing St. London") ) withSchema USER_DATA_SCHEMA
        } When {
          (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
        } Then {
          buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
            buffer.toList shouldEqual List( ("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l) )
        }
    }
  }
}
```

or ScalaTest

```scala
class SampleJobPipeTransformationsSpec extends FlatSpec with ShouldMatchers with TupleConversions with BddDsl {
  "A sample job pipe transformation" should "add user info" in {
    Given {
      List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
    } And {
      List( (1000002l, "stefano@email.com", "10 Downing St. London") ) withSchema USER_DATA_SCHEMA
    } When {
      (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        buffer.toList shouldEqual List( ("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l) )
    }
  }
}
```

or with Specs2

```scala
import org.specs2.{mutable => mutableSpec}

class SampleJobPipeTransformationsSpec2Spec extends mutableSpec.SpecificationWithJUnit with TupleConversions with BddDsl {

  // See: https://github.com/twitter/scalding/wiki/Frequently-asked-questions

  "A sample job pipe transformation" should {
     Given {
      List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
    } And {
      List((1000002l, "stefano@email.com", "10 Downing St. London")) withSchema USER_DATA_SCHEMA
    } When {
      (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        "add user info" in {
          buffer.toList shouldEqual List(("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l))
        }
    }

  }
}
```

Where `addUserInfo` is a function joining two pipes to generate an enriched one.

An example using the Typed API and Specs2, both using tuples or more complex types

```scala
case class UserInfo(name: String, gender: String, age: Int)
case class EstimatedContribution(name: String, suggestedPensionContributionPerMonth: Double)

class TypedApiTest extends Specification with TBddDsl {

  "A test with a single source" should {

    "accept an operation from working with a single tuple-typed pipe" in {
      Given {
        List(("Joe", "M", 40), ("Sarah", "F", 22))
      } When {
        in: TypedPipe[(String, String, Int)] =>
          in.map[(String, Double)] { person =>
            person match {
              case (name, "M", age) => (name, (1000.0 / (72 - age)).toDouble)
              case (name, _, age) => (name, (1000.0 / (80 - age)).toDouble)
            }
          }
      } Then {
        buffer: mutable.Buffer[(String, Double)] =>
          buffer.toList mustEqual List(("Joe", 1000.0 / 32), ("Sarah", 1000.0 / 58))
      }
    }
    
    "accept an operation from single case class-typed pipe" in {
      Given {
        List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      } When {
        in: TypedPipe[UserInfo] =>
          in.map { person =>
            person match {
              case UserInfo(name, "M", age) => EstimatedContribution(name, (1000.0 / (72 - age)))
              case UserInfo(name, _, age) => EstimatedContribution(name, (1000.0 / (80 - age)))
            }
          }
      } Then {
        buffer: mutable.Buffer[EstimatedContribution] =>
          buffer.toList mustEqual List(EstimatedContribution("Joe", 1000.0 / 32), EstimatedContribution("Sarah", 1000.0 / 58))
      }
    }
  }   
}
```

## Motivation and details

Please note that the discussion describes an example using the field API but everything maps to the typed API. 

A Scalding job consists in a series of transformations applied to one or more sources in order to create one or more
output resources or sinks. A very simple example taken from the Scalding documentations is as follows.

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
    TextLine( args("input") )
     .flatMap('line -> 'word) { line : String => tokenize(line) }
     .groupBy('word) { _.size }
     .write( Tsv( args("output") ) )

    // Split a piece of text into individual words.
    def tokenize(text : String) : Array[String] = {
     // Lowercase each word and remove punctuation.
     text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
    }
}
```

The transformations are defined as operations on a `cascading.pipe.Pipe` class or on the richer wrapper `com.twitter.scalding.RichPipe`.
Scalding provides a way of testing Jobs via the com.twitter.scalding.JobTest class. This class allows to specify values for the different
Job sources and to specify assertions on the different job sinks.
This approach works very well to do end to end test on the Job and is good enough for small jobs as the one described above
but doesn't encourage modularisation when writing more complex Jobs.

When the Job logic become more complex it is very helpful to decompose its work in simpler functions to be tested independently before being
aggregated into the Job.

Let's consider the following Scalding Job (it is still a simple one for reason of space but should give the idea):

```scala
class SampleJob(args: Args) extends Job(args) {
  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val WITH_DAY_SCHEMA = List('date, 'userid, 'url, 'day)
  val EVENT_COUNT_SCHEMA = List('day, 'userid, 'event_count)
  val OUTPUT_SCHEMA = List('day, 'userid, 'email, 'address, 'event_count)

  val USER_DATA_SCHEMA = List('userid, 'email, 'address)

  val INPUT_DATE_PATTERN: String = "dd/MM/yyyy HH:mm:ss"

  Osv(args("eventsPath")).read
    .map('date -> 'day) {
           date: String => DateTimeFormat.forPattern(INPUT_DATE_PATTERN).parseDateTime(date).toString("yyyy/MM/dd");
         }
    .groupBy(('day, 'userid)) { _.size('event_count) }
    .joinWithLarger('userid -> 'userid, Osv(args("userInfoPath")).read).project(OUTPUT_SCHEMA)
    .write(Tsv(args("outputPath")))
}
```

It is possible to identify three main operation performed during this task. Each one is providing a identifiable and
 autonomous transformation to the pipe, just relying on a specific input schema and generating a transformed pipe with a
 potentially different output schema. Following this idea it is possible to write the Job in this way:

```scala
class SampleJob(args: Args) extends Job(args) {
  import SampleJobPipeOperationsWrappers._
  import sampleJobSchemas._

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addDayColumn
    .countUserEventsPerDay
    .addUserInfo(Osv(args("userInfoPath"), USER_DATA_SCHEMA).read)
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )
}
```

Where the single operations have been extracted as basic functions into a separate class that is not a Scalding Job:

```scala
package object sampleJobSchemas {
  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val WITH_DAY_SCHEMA = List('date, 'userid, 'url, 'day)
  val EVENT_COUNT_SCHEMA = List('day, 'userid, 'event_count)
  val OUTPUT_SCHEMA = List('day, 'userid, 'email, 'address, 'event_count)

  val USER_DATA_SCHEMA = List('userid, 'email, 'address)
}


trait SampleJobPipeOperations extends FieldConversions with TupleConversions {
  import sampleJobSchemas._
  import Dsl._

  val INPUT_DATE_PATTERN: String = "dd/MM/yyyy HH:mm:ss"

  def self: Pipe

  /* Input schema: INPUT_SCHEMA
   * Output schema: WITH_DAY_SCHEMA */
  def addDayColumn : Pipe = self.map('date -> 'day) {
    date: String => DateTimeFormat.forPattern(INPUT_DATE_PATTERN)
      .parseDateTime(date).toString("yyyy/MM/dd");
  }

  /** Input schema: WITH_DAY_SCHEMA
    * Output schema: EVENT_COUNT_SCHEMA */
  def countUserEventsPerDay : Pipe =
    self.groupBy(('day, 'userid)) { _.size('event_count) }

  /** Joins with userData to add email and address
    *
    * Input schema: WITH_DAY_SCHEMA
    * User data schema: USER_DATA_SCHEMA
    * Output schema: OUTPUT_SCHEMA */
  def addUserInfo(userData: Pipe) : Pipe =
    self.joinWithLarger('userid -> 'userid, userData).project(OUTPUT_SCHEMA)
}

object SampleJobPipeOperationsWrappers {

  implicit def wrapPipe(self: Pipe): SampleJobPipeOperationsWrapper =
    new SampleJobPipeOperationsWrapper(new RichPipe(self))

  implicit class SampleJobPipeOperationsWrapper(val self: RichPipe) extends SampleJobPipeOperations
}
```

The main Job class is responsible of essentially dealing with the configuration, opening the input and output pipes and
combining those macro operations. The `SampleJobPipeOperationsWrappers` is just defining a set of implicit conversion to
allow you to use the methods in `SampleJobPipeOperations` as extension methods of `Pipe` or `RichPipe` classes.

It is now possible to test all this method independently and without caring about the source and sinks of the job and of
the way the configuration is given.

The specification of the transformation class is shown below:

```scala
class SampleJobPipeTransformationsSpec extends FlatSpec with ShouldMatchers with TupleConversions with BddDsl {
  "A sample job pipe transformation" should "add column with day of event" in {
    Given {
      List( ("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com") ) withSchema INPUT_SCHEMA
    } When {
      pipe: RichPipe => pipe.addDayColumn
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        buffer.toList(0) shouldEqual (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
    }
  }

  it should "count user events per day" in {
    def sortedByDateAndIdAsc( left: (String, Long, Long), right: (String, Long, Long)): Boolean =
      (left._1 < right._1) || ((left._1 == right._1) && (left._2 < left._2))

    Given {
      List(
          ("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"),
          ("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"),
          ("11/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/11"),
          ("15/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/15"),
          ("15/02/2013 10:22:11", 1000001l, "http://www.youtube.com", "2013/02/15"),
          ("15/02/2013 10:22:11", 1000003l, "http://www.youtube.com", "2013/02/15"),
          ("15/02/2013 10:22:11", 1000001l, "http://www.youtube.com", "2013/02/15"),
          ("15/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/15")
        ) withSchema WITH_DAY_SCHEMA
    } When {
      pipe: RichPipe => pipe.countUserEventsPerDay
    } Then {
      buffer: mutable.Buffer[(String, Long, Long)] =>
        buffer.toList.sortWith(sortedByDateAndIdAsc(_, _)) shouldEqual List(
                ("2013/02/11", 1000002l, 1l),
                ("2013/02/12", 1000002l, 2l),
                ("2013/02/15", 1000001l, 2l),
                ("2013/02/15", 1000002l, 2l),
                ("2013/02/15", 1000003l, 1l)
              )
    }
  }


  it should "add user info" in {
    Given {
      List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
    } And {
      List( (1000002l, "stefano@email.com", "10 Downing St. London") ) withSchema USER_DATA_SCHEMA
    } When {
      (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        buffer.toList shouldEqual List( ("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l) )
    }
  }
}
```

Once the different steps have been tested thoroughly it is possible to combine them in the main Job and test the end to end
behavior using the JobTest class provided by Scalding.




