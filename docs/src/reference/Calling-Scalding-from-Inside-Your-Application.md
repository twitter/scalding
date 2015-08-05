# Calling Scalding from Inside your Application

Starting in scalding 0.12, there is a clear API for doing this. See `Execution[T]`, which describes a set of map/reduce operations that when executed return a `Future[T]`. See the [scaladocs for Execution](http://twitter.github.io/scalding/#com.twitter.scalding.Execution). Below is an example.

```scala
val job: Execution[Unit] =
  TypedPipe.from(TextLine("input"))
    .flatMap(_.split("\\s+"))
    .map { word => (word, 1L) }
    .sumByKey
    .writeExecution(TypedTsv("output"))
// Now we run it in Local mode
val u: Unit = job.waitFor(Config.default, Local(true))

// Or for Hadoop:
val jobConf = new JobConf
val u: Unit = job.waitFor(Config.hadoopWithDefaults(jobConf), Hdfs(true, jobConf))
// If you want to be asynchronous, use run instead of waitFor and get a Future in return
```
For testing or cases where you aggregate data down to a manageable level, `.toIterableExecution` on TypedPipe is very useful:

```scala
val job: Execution[Iterable[(String, Long)]] =
  TypedPipe.from(TextLine("input"))
    .flatMap(_.split("\\s+"))
    .map { word => (word, 1L) }
    .sumByKey
    .toIterableExecution
// Now we run it in Local mode
val counts: Map[String, Long] = job.waitFor(Config.default, Local(true)).toMap
```

To run an Execution as a stand-alone job, see:

1. [ExecutionApp](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/ExecutionApp.scala#L75) Make an `object MyExJob extends ExecutionApp` for a job you can run like a normal java application (by using java on the classname).
2. [ExecutionJob](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/Job.scala#L441) - use this only if you have an existing tooling around launching scalding.Job subclasses.

## Some rules
1. When using Execution NEVER use `.write` or `.toPipe` (or call any method that takes an implicit flowDef). Instead use `.writeExecution`, `.toIterableExecution`, or `.forceToDiskExecution`. (see [scaladocs](http://twitter.github.io/scalding/#com.twitter.scalding.Execution)).

2. Avoid calling `.waitFor` or `.run` AS LONG AS POSSIBLE. Try to compose your entire job into on large Execution using `.zip` or `.flatMap` to combine `Executions`. `waitFor` is the same as `run` except it waits on the future. There should be at most 1 calling to .waitFor or .run in each Execution App/Job.

3. Only mutate vars or perform side effects using `.onComplete`. If you `run` the result of `onComplete`, your function you pass will be run when the result up to that point is available and you will get the `Try[T]` for the result. Avoid this if possible. It is here to deal with external IO, or existing APIs, and designed for experts that are comfortable using .onComplete on scala Futures (which is all this method is doing under the covers).

## Running Existing Jobs Inside A Library

We recommend the above approach to build composable jobs with Executions. But if you have an existing Job, you can also run that:

### Working example:
**WordCountJob.scala**
```scala
class WordCountJob(args: Args) extends Job(args) {
  TextLine(args("input"))
    .read
    .flatMap('line -> 'word) { line: String => line.split("\\s+") }
    .groupBy('word) { _.size }
    .write(Tsv(args("output")))
}
```

**Runner.scala**
```scala
object Runner extends App {
  val hadoopConfiguration: Configuration = new Configuration
  hadoopConfiguration.set("mapred.job.tracker","hadoop-master:8021")
  hadoopConfiguration.set("fs.defaultFS","hdfs://hadoop-master:8020")

  val hdfsMode = Hdfs(strict = true, hadoopConfiguration)
  val arguments = Mode.putMode(hdfsMode, Args("--input in.txt --output counts.tsv"))

  // Now create the job after the mode is set up properly.
  val job: WordCountJob = new WordCountJob(arguments)
  val flow = job.buildFlow
  flow.complete()
}
```

And then you can run your App on any server, that have access to Hadoop cluster
