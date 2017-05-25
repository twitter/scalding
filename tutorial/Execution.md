# A Random Walk Down Executions or: You Could Have Invented Executions or: Learn You An Execution For Greater Good

The following is a guide to understanding Scalding's [com.twitter.scalding.Execution](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/Execution.scala) type.

## What is a Scalding Execution?

A Scalding job (lowercase 'j') is the distributed completion of a DAG of Hadoop jobs that read, transform, and write data, usually to the Hadoop Distributed File System. A Scalding `Execution` is conceptually a plan to run zero or more Scalding jobs. The Scalding library has another paradigm, the Scalding `Job` class, that enables users to plan a single Scalding job in the constructor of a class. `Execution` is a newer, more composable, and more functional approach to running Scalding jobs.

## Why would I want to use an Execution?

The Scalding `Job` class uses a more object oriented programming approach to planning Scalding jobs, which often makes it more difficult to reason about and extend functionality. In addition, running multiple Scalding `Job` instances is poorly supported.

Scalding `Execution`, on the other hand, is inspired by functional programming and composes easily. Here are a few things that are much easier with Scalding `Execution`:

* Calling out to a service asynchronously to read or write some state before or after a Scalding job runs
* Running multiple Scalding jobs or not running any Scalding jobs at all
* Using the output of one Scalding job to decide how to plan future jobs
* Running one Scalding job repeatedly until some condition is met, such as for Machine Learning applications
* Unit testing

These situations come up frequently when integrating Scalding jobs into an analytics workflow.

## Reasoning about how Executions work

A good starting model for thinking about Scalding `Execution` is scala's asynchronous primitive `Future`, which is a container for some value that will either be available in the future or throw an `Exception`. A Scalding `Execution` is similar in that it computes a value in a analytics-focused environment or throws an `Exception`. Like `Future`, `Execution`s can also can be chained together for great effect (as we'll examine later); however, a fundamental difference is that a `Future` represents a computation that has already been scheduled, whereas an `Execution` is a plan to run some computation. For now, we will use a simplified version of `Execution` to understand it.

At its heart, a Scalding `Execution` wraps a function that takes in a Scalding `Config` and a Scalding `Mode` and does *something*. A `Config` contains configuration properties for the running of a Scalding job. The `Mode` tells the Scalding job what environment (in-memory, in a local Hadoop cluster, in a remote Hadoop cluster) to run under. The Scalding `Mode` also contains an `Args` object, which represent Scalding-specific arguments passed on the command line.

Here's our simplified `Execution`:

```scala
import com.twitter.scalding.{Config, Mode}

case class Execution[T](private doSomething: (Config, Mode) => T) {
  def run(config: Config, mode: Mode): T = {
    this.doSomething(config, mode)
  }
}

object Execution {
  def getConfig: Execution[Config] = {
    def newDoSomething(config: Config, mode: Mode) = config

    Execution(newDoSomething)
  }

  def getMode: Execution[Mode] = {
    def newDoSomething(config: Config, mode: Mode) = mode

    Execution(newDoSomething)
  }
}
```

So, the function `doSomething` above takes in a `Config` and `Mode` and gives us a `T`. It is encapsulated inside of `Execution`, so we cannod access it directly, but only call the `run` method to invoke it.

There's also a companion object `Execution` that enables us get the `Config` or `Mode`. A use of `Execution` might look like:

```scala
val config: Config = ???
val mode: Mode = ???

Execution.getConfig.run(config, mode)  // Returns the config
```

Not very useful yet.

### Map

Just like scala's `Seq`, `Option`, `Future`, etc., `Execution` can be *mapped over* with its `map` method. In the context of `Seq`, calling the `map` method will transform every element into a new element via some function. In `Execution`, the `map` method gives us a new `Execution` that will produce the value of the old `Execution` and then transform that value with the function. Remember, `Execution` is a plan to do something; it won't be run until we call the `run` method:

```scala
case class Execution[T](private doSomething: (Config, Mode) => T) {
  def map[U](transform: T => U): Execution[U] = {
    def newDoSomething(config: Config, mode: Mode): U = {
      val result = this.doSomething(config, mode))
      transform(result)
    }

    Execution(newDoSomething)
  }

  // ... other previously defined methods ...
}
```

This is pretty powerful. `Execution` can now be used to access anything in the `Config` or `Mode`, or even do arbitrary work:

```scala
val config: Config = ???
val mode: Mode = ???

val funProperty: String = Execution
  .getConfig
  .map { config => config.get("fun") }
  .run(config, mode)  // Returns the config value for "fun"

val fivePlusThree = Execution
  .getMode
  .map { _ => 5 + 3 }
  .run(config, mode)  // Returns 8
```

### From

We take an arbitrary scala expression and wrapping it in `Execution` so frequently that we're going to add a helper method, `from`, for it:

```scala
object Execution {
  def from[T](value: => T): Execution[T] = {
    def newDoSomething(config: Config, mode: Mode) = value
    Execution(newDoSomething)
  }

  // ... other previously defined methods ...
}
```

Why is it `value: => T` and not `value: T`? This scala syntax here `=>` is "call by name" and is essentially receiving a function of 0 parameters. Check out the scala documentation for more.

At this point, we can write an `Execution` that reads from a service and uses its result to do something:

```scala
val config: Config = ???
val mode: Mode = ???
val service: String => String = ???

val callOutToService = Execution.from {
  service("eat galaxies")
}

val reversedServiceResult = callOutToService.map { serviceResult =>
  serviceResult.reverse
}

reversedServiceResult.run(config, mode)  // Returns the reversed serviceResult
```

### Running a Scalding job

Writing a single Scalding job with `TypedPipe` and `Grouped` will not be covered in this tutorial; however, in order to run Scalding jobs with `Execution`, we need to be familiar with with 2 methods on `TypedPipe`: `writeExecution` and `toIterableExecution`:

```scala
val config: Config = ???
val mode: Mode = ???

val source: TypedSource[Int] = ???
val sink: TypedSink[String] = ???

val scaldingJob: Execution[Unit] = TypedPipe
  .from(source)
  .map { i => i.toString }
  .writeExecution(sink)

scaldingJob.run(config, mode)  // Returns ()
```

The `TypedPipe` method `writeExecution` parallels the normal `write` method on `TypedPipe`, but instead of planning the result inside of a Scalding `Job` class, `writeExecution` yields a plan to run that Scalding job as an `Execution`. The type parameter is `Unit` because the purpose of the `Execution` created from the `TypedPipe` is to have side-effects by writing to the `TypedSink` argument.

```scala
val config: Config = ???
val mode: Mode = ???

val source: TypedSource[Int] = ???
val sink: TypedSink[String] = ???

val scaldingJobResults: Execution[Iterable[String]] = TypedPipe
  .from(source)
  .map { i => i.toString }
  .toIterableExecution

scaldingJobResults
  .map { results => results.foreach(println) }
  .run(config, mode)  // Returns ()
```

The `TypedPipe` method `toIterableExecution` creates an `Execution` plan to expose the output of the `TypedPipe` in the local environment. This is useful if we need to use the output of a Scalding job to decide what to do next. Note that there's no guarantee on the order of the data and for large datasets, the length of the `Iterable` could be significant.

### Flatmap

So far, we've seen how to examine the `Config`, `Mode`, lift an arbitrary value into an `Execution` object, and run a Scalding job. We've alluded to the idea that we can use the output of one `Execution` to plan another, but haven't talked about how to do that. One may be familiar with the method `flatMap` on various types in scala, just like `map`. `flatMap` behaves differently depending on the type.

For example, an `Option` contains either one value (`Some`) or no (`None`) values. When we `flatMap` on an `Option`, if there's a value in the `Option` (`Some`), the function we pass to `flatMap` examines the value inside the `Option` to produce a new `Option`:

```scala
def isEven(i: Int) = i % 2 == 0
val maybePrimes = Some(Seq(2, 3, 5))                     // Some(Seq(2, 3, 5))
val maybeFirstEven = maybePrimes.flatMap(_.find(isEven)) // Some(2)
```

`Execution`s focus on planning analytical work (like Scalding jobs), so `Execution`'s `flatMap` method behaves analogously to `Option`'s and enables us to use the result of one `Execution` to plan another:

```scala
case class Execution[T](private doSomething: (Config, Mode) => T) {

  def flatMap[U](planNextExecution: T => Execution[U]) = {

    def newDoSomething(config: Config, mode: Mode): U = {
      val firstResult = this.doSomething(config, mode)
      val nextExecution = planNextExecution(firstResult)
      nextExecution.run(config, mode)
    }

    Execution(newDoSomething)
  }

  // ... other previously defined methods ...
}
```

Again, remember the `Execution` is just a plan until we call the `run` method with a `Config` and `Mode`. So after calling `flatMap`, the new `Execution` will create a plan to run itself, plan a new `Execution` based on the result of running itself, and then immediately run the newly planned `Execution`. ZOMG!

At this point, we have most of the methods necessary to do some pretty useful work:

```scala
val config: Config = ???
val mode: Mode = ???
val loggingService: String => Unit = ???

val announceStart = Execution.from(loggingService("Starting job"))
val announceStop = Execution.from(loggingService("Stopping job"))
val scaldingJob = TypedPipe
  .from(/* ... */)
  .map(/* ... */)
  .writeExecution(/* ... */)

val scaldingJobWithAnnouncements =
  announceStart.flatMap { _ =>
    scaldingJob.flatMap { _ =>
      announceStop
    }
  }

scaldingJobWithAnnouncements.run(config, mode)  // Returns ()
```

As always, remember that we are creating a plan to run a Scalding job (and logging service announcements). not actually running this code. Until we've called `Execution`'s `run` method, no work has been done (besides instantiating the loggingService).

While the code we run in the `TypedPipe` methods `map`, `filter`, etc. may happen in a different run environment according on the `Mode` (e.g. remote Hadoop cluster), everything else is happening locally such as the calls to the `loggingService`.

Also note that we have to call `flatMap` twice here. If we had written

```scala
val scaldingJobWithAnnouncements =
  announceStart.flatMap {
    scaldingJob
    announceStop
  }
}
```

the `scaldingJob` `Execution` would be "orphaned" and never run. We would be constructing a plan to run `scaldingJob`, but then throwing it away and making `announceStop` follow after `announceStart` because it is the value that is returned.

## The Real Execution

Thus far, we have only discussed the simplified version of `Execution` to make it easier to understand. Here, we will talk about the real `Execution` in Scalding and how to make best use of it.

### Executions are actually asynchronous

Our `case class Execution` above does all work synchronously--only one thing at a time. In reality, `Execution` uses scala's `scala.concurrent.ExecutionContext` to schedule tasks asynchronously. So, if we want to turn non-Scalding asynchronous tasks into planned `Execution`s for use with Scalding, we can use the `Execution` companion object's method `def fromFuture[T](fn: ExecutionContext => Future[T]): Execution[T]` to schedule a scala `Future` on the same `ExecutionContext` that the `Execution` is using:

```scala
Execution.fromFuture { implicit executor: ExecutionContext =>
  Future { /* ... asynchronous task ... */ }
}
```

Additionally, the `run` method from our simplified `Execution` above, in the real `Execution`, takes in a scala `ExecutionContext` and yields a `Future`, so we can actually run `Execution` alongside other asynchronous scala code:

```scala
val config: Config = ???
val mode: Mode = ???

val scaldingJob: Execution[Unit] = ???
implicit val executor: ExecutionContext = ???

Future { /* ... asynchronous task ... */ }
  .flatMap{ _ => scaldingJob.run(config, mode)(executor) }
```

But in general, we recommend that one extends `ExecutionApp` (below) and plans asynchronous actions from within that environment instead of integrating `Execution`s into existing asynchronous environments.

### ExecutionApp

Hadoop workflows often use key-value properties (e.g. `-Dmapred.min.split.size=1073741824`) passed to the `hadoop` command line to tweak aspects of how the Hadoop job(s) in the Scalding job will run. Additionally, if one is accustomed to using the Scalding `Job` class, it may not be apparent how to query the `Args` object to decide how to build the `TypedPipe`s. The `Execution` API provides this through a main class `ExecutionApp`:

```scala
trait ExecutionApp {

  def job: Execution[Unit]

  // Subclasses can be used as the main class for the JVM
  final def main(args: Array[String]): Unit = // ...

  // ... other defined methods ...
}
```

For `ExecutionApp`, we need to implement the `job` method with our `Execution` and `ExecutionApp`'s `main` method will parse the command line, instantiate the `Config` and `Mode` objects, and call our `Execution`.

In the `Execution` API, to access the `Args` object inside of the `Mode` object, there is a convenience method `getArgs` on the `Execution` companion object:

```scala
import com.twitter.scalding.{DateOps, DateParser}

object MyExecutionApp extends ExecutionApp {
  override def job: Execution[Unit] = Execution.getArgs.flatMap { args =>

    // This is similar to the functionality in Scalding's DefaultDateRangeJob
    val dateRange = DateRange.parse(args.list("date"))(DateOps.UTC, DateParser.default)

    MyExecution.fromDateRange(dateRange)
  }
}

object MyExecution {
  def fromDateRange(dateRange: DateRange): Execution[Unit] = {
    val source: DateRange => TypedSource[T] = ???
    val sink: DateRange => TypedSink[T] = ???

    TypedPipe
      .from(source(dateRange))
      .map(/* ... */)
      .writeExecution(sink(dateRange))
  }
}
```

### Handling failures

Much like scala's `Try` and `Future` types, `Execution`s can either succeed with some result or fail with a `Throwable`. There are methods on `Execution` to handle failures and recover from them:

```scala
def withRetries(retries: Int)(execution: Execution[Unit]): Execution[Unit] = {
  execution.recoverWith {
    case t: Throwable if retries > 0 =>
      logger.warning(t, s"Failed to run execution. Retrying $retries more time(s).")
      withRetries(retries - 1)(Execution.withNewCache(execution))
  }
}
```

The method `recoverWith` parallels the method by the same name in `Future`: it takes a `PartialFunction` that enables us to recover from specific failure cases with another `Execution` (if the `PartialFunction` matches). In our example above, we are retrying an `Execution` a finite number of times. Read the section below on the cache for more information on `Execution.withNewCache`.

### Execution cache

When running an `Execution`, it keeps a cache of the previously computed `Execution` results (the values exposed in the `map` and `flatMap` methods). This is handy because Scalding jobs are expensive to re-compute. Specifically, the cache is keyed on `(Config, Execution)`, so if we *want* to repeat an `Execution` with side-effects, wrapping an `Execution` with `Execution.withNewCache` will invalidate the cache for that `Execution` and ensure that it is re-computed.

### Parallelism

To run multiple `Execution`s at the same time, there is a method `zip` defined both on the `Execution` type and companion object. The `zip` method will run multiple `Execution`s concurrently and yield their results once all the `Execution`s have finished as a tuple:

```scala
val sumExec: Execution[Double] = ???
val countExec: Execution[Long] = ???

val averageExec: Execution[Double] = Execution
  .zip(sumExec, countExec)
  .map { case (sum, count) => sum / count }
```

There's a convenience method on the `Execution` companion object called `sequence` that will run a `Seq[Execution]` in parallel and expose the results as a `Seq`. Additionally, the method `withParallelism` enables running only a specific number of a `Seq[Execution]` at once:

```scala
val count: Int => Execution[Int] = ???

val counts: Seq[Execution[Int]] = Seq.fill(10)(count)

Execution
  .sequence(counts)
  .map { numbers => numbers.sum }

Execution
  .withParallelism(counts, parallelism = 3)
  .map { numbers => numbers.sum }
```

### Config changes

In each example above, the `Config` object never changes. Sometimes, we desire to set a `Config` property for only one `Execution`, but not the others. `Execution.withConfig` applies a transformation to the `Config` object, localized to one `Execution`:

```scala
val execution: Execution[Unit] = ???

Execution.withConfig(execution){ config =>
  config + ("mapred.min.split.size", "67108864")
}
```

### Unit Testing

If we make good use of dependency injection (passing a function's dependencies to it), testing `Execution`s is not terribly difficult. That being said, much of the complexity is in the Scalding `TypedPipe`, so it often times may seem like overkill to write unit tests for `Execution`s (and often is) after writing them for a `TypedPipe` transformation function.

When running `Execution`s in a test environment, it's useful to use the `waitFor` method on `Execution` to block on an `Execution` to finish. Also, just like for the Scalding `Job` class, defining our Scalding job in terms of transformations on `TypedPipe`s makes it easy to test that functionality with `TypedPipeChecker`:

```scala
// Code
object EvensExecution {
  def onLongs(longs: TypedPipe[Long], sink: TypedSink[Double]): Execution[Unit] = {
    keepEvens(longs).writeExecution(sink)
  }

  def keepEvens(longs: TypedPipe[Long]): TypedPipe[Long] = {
    longs.filter(_ % 2 == 0)
  }
}

// Test
import com.twitter.scalding.TypedPipeChecker
import com.twitter.scalding.typed.MemorySink
import org.scalatest.WordSpec
import scala.util.Success

class EvensExecutionTest extends WordSpec {
  "EvensExecution" should {
    "write only evens" in {
      val numbers = Seq(1, 2)
      val expected = Success(Seq(2))

      val sink = new MemorySink[Long]

      val exec = EvensExecution.onLongs(TypedPipe.from(numbers), sink)

      val result = Execution.waitFor(Config.default, Local(true))

      assert(result == Success(()))
      assert(sink.readResults == expected)
    }

    "filter out odds" in {
      val numbers = Seq(1, 2)
      val expected = Seq(2)

      val pipe = EvensExecution.keepEvens(TypedPipe.from(numbers))
      val result = TypedPipeChecker.inMemoryToList(pipe).toSeq

      assert(result == expected)
    }
  }
}
```
