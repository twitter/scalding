# Scalding REPL

In addition to production batch jobs, Scalding can be run interactively to help develop ad-hoc queries or give beginners a chance to learn about the platform step-by-step.

The Tutorial will walk through using the REPL in depth. Users already familiar with Scalding may wish to skip to the summary of REPL functionality below.

## Quick Start
Starting up the REPL in local mode is as easy as:

~~~ scala
> ./sbt "scalding-repl/run --local"
~~~

To run full HDFS mode on a remote machine, you must build the assembly jar and use the `scald.rb` script to rsync and ssh to the right machine:

~~~ scala
> ./sbt assembly
> ./scripts/scald.rb --repl --hdfs --host <host to ssh to and launch jobs from>
~~~

## Tutorial

Assuming you've checked out the Scalding code, the fastest way to start running Scalding is to launch the REPL in Local mode. Simply run:

    > ./sbt "scalding-repl/run --local"

It will spend some time downloading and compiling, but should eventually show:

```scala
[info] Set current project to scalding (in build file:/Users/bholt/hub/scalding/)
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
...
scalding>
```

```tut:invisible
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
```

As you can see, we've imported a bunch of Scalding code, including implicits that make it easier to run jobs interactively. Several of these enhancements are "enrichments" on Scalding's `TypedPipe`. The tutorial will go into them in more detail.

Let's take a look at some.

### Viewing pipe contents
First, `dump` allows you to print what's in a TypedPipe:

```tut
// load a plain text file into a TypedPipe
val hello = TypedPipe.from(TextLine("tutorial/data/hello.txt"))
hello.dump
```

We can also load the contents into memory as a list:

```tut
val lst = hello.toList
```

In fact, both `dump` and `toList` use another TypedPipe enrichment, `toIterator`, which can be used directly if one wishes to stream over the data and perform some operation.

Remember that each of these operations run *locally*, so if you *are* actually working with large datasets, ensure that you do most of your work using TypedPipe operations, which will run through Cascading/Hadoop, and only call `toIterator` (or `dump`, or `toList`) once you've filtered the results sufficiently. Some helpful TypedPipe operations to downsize datasets are `limit` and `sample`.

### Save
Usually in Scalding, results are written to a Sink using `.write()`. However, the write only happens when the flow is actually run. That way you can build up a complicated flow with multiple sources and sinks and intermediate calculations and run it as one big job. When running interactively, however, we want a way to immediately run part of a job to see if it's correct before moving on. When running in the REPL, `save` can be used in place of `write` to immediately run that pipe and write out the results.

```tut
val hello = TypedPipe.from(TextLine("tutorial/data/hello.txt"))

val words = hello.flatMap(_.split("\\s+")).save(TypedTsv("words.tsv"))
```

Conviently, this returns a new pipe reading from the file which can be used in subsequent computations.

### Snapshots
When working interactively with large datasets, it is often useful to be able to save and re-use data that took significant work to generate. The Scalding REPL provides an easy way to explicitly save intermediate results and use these results in subsequent interactive work.

The `snapshot` enrichment on TypedPipe runs everything necessary to generate the output for the given pipe and saves it to temporary storage. In Local mode, it actually just saves it in memory; in Hadoop, it writes it out to a temporary SequenceFile. Scalding then returns a handle to this new Source as a new TypedPipe.

Snapshots should only be used within a single REPL session. In local mode, you have no choice because they only exist in memory. But even for Hadoop mode, it is not a good idea to try to re-use generated snapshots, as Kryo serialization is unstable and the temporary files may be cleaned up when not in use.

```tut
// (using 'hello' TypedPipe declared above)
// split into words, returns TypedPipe all set to do the work (but not run yet)
val words = hello.flatMap(_.split("\\s+")).map(_.toLowerCase)

// save a snapshot
val s = words.snapshot

// now take a look at what's in the snapshot
s.dump
```

You can see to create the snapshot, it ran a small Cascading job, and returned a TypedPipe with the result to us. Now, rather than spending that enormous amount of time to re-generate "words", we can use `s`, our snapshot, to, for a change, count some words:

```tut
val wordCount = s.map((_, 1)).sumByKey

wordCount.snapshot.dump
```

Notice above how running `wordCount` with `snapshot` used the MemoryTap (snapshot) created before as its source. If, instead, we wanted to run the entire job end-to-end, we simply need to bypass the snapshots and use the original pipes:

```tut
// remember, 'words' was a TypedPipe which reads from the original text file
words

words.map((_, 1)).sumByKey.dump
```

See, that time the "source" was the original text file.

#### Implicit snapshots
The previous example actually pulled out a subtle additional trick. Calling `dump` on the result of `sumByKey` ran the flow *and* printed the results. Because you may be dealing with large datasets, we want to run these pipes using Cascading in Hadoop. The `toIterator` method, which is called by `dump`, doesn't want to have to iterate over excessively large datasets locally, so it calls `snapshot`, which runs the flow in Cascading, and then `toIterator` can iterate over the snapshot. However, each call to `dump`  as above will need to re-run, because we aren't keeping a handle to the snapshot around. If a flow is taking a while to run, try saving the results to a snapshot first, then using that snapshot to further understand the data.

## Running on Hadoop
So far, we've only been running in Cascading's Local mode, which emulates what Hadoop will do, but cannot handle actually large datasets. The easiest way to launch the REPL in Hadoop mode is to use the `scald.rb` script.

```bash
# must first create the massive assembly jar with all the code
scalding$ ./sbt assembly
# then launch the REPL
scalding$ ./scripts/scald.rb --repl --hdfs
```
