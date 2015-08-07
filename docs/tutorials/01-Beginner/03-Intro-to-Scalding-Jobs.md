# Intro to Scalding Jobs

## WordCount in Scalding

Let's look at a simple WordCount job.

```scala
import com.twitter.scalding._

class WordCountJob(args: Args) extends Job(args) {
  TypedPipe.from(TextLine(args("input")))
    .flatMap { line => line.split("""\s+""") }
    .groupBy { word => word }
    .size
    .write(TypedTsv(args("output")))
}
```

This job reads in a file, emits every word in a line, counts the occurrences of each word, and writes these word-count pairs to a tab-separated file.

To run the job, copy the source code above into a `WordCountJob.scala` file, create a file named `someInputfile.txt` containing some arbitrary text, and then enter the following command from the root of the Scalding repository:

```bash
scripts/scald.rb --local WordCountJob.scala --input someInputfile.txt --output ./someOutputFile.tsv
```

This runs the WordCount job in _local_ mode (i.e., not on a Hadoop cluster). After a few seconds, your first Scalding job should be done!

## WordCount dissection

Let's take a closer look at the job.

### TextLine

`TextLine` is an example of a Scalding [source](http://www.cascading.org/1.2/userguide/html/ch03s03.html) that reads each line of a file into a field named `line`.

```scala
TextLine(args("input")) // args("input") contains a filename to read from
```

Another common source is a `TypedTsv` source that reads tab-delimited files. You can also create sources that read directly from LZO-compressed files on HDFS (possibly containing Protobuf- or Thrift-encoded objects!), or even database sources that read directly from a MySQL table. See the scalding-commons module for lzo, thrift and protobuf support. Also see scalding-avro if you use avro.

### flatMap

`flatMap` is an example of a function that you can apply to a stream of tuples.

```scala
TypedPipe.from(TextLine(args("input")))
  // flat map the "line" field to a new words separated by one or more space
  .flatMap { line => line.split("""\s+""") }
```

The above works just like calling flatMap on a List in scala.

Our tuple stream now contains something like the following:

    input             output
    this is a line    this
    line 2            is
                      a
                      line
                      line
                      2

See the [[Type-safe api reference]] for more examples of `flatMap` (including how to flat map from and to multiple fields), as well as examples of other functions you can apply to a tuple stream.

### groupBy

Next, we group the same words together, and count the size of each group.

```scala
TypedPipe.from(TextLine(args("input")))
  .flatMap { line => line.split("""\s+""") }
  .groupBy { word => word }
  .size
```

Here, we group the stream into groups of tuples with the same `word`, and then we make the value for each keyed group the size of that group.

The tuple stream now looks like:

    (line, 2)
    (this, 1)
    (is, 1)
    ...

Again, see the [[Type-safe api reference]]  for more examples of grouping functions.

### write, Tsv

Finally, just as we read from a `TextLine` source, we can also output our computations to a `TypedTsv` source.
A TypedTsv can see the types it is putting in each column. We could write `TypedTsv[(String, Long)]` below
to be sure we are writing what we intend, but scala can usually infer the types if we leave them off (though, when you get in trouble, try adding the types near the compilation error and see if you can get a better message as to what is going on).

```scala
  TypedPipe.from(TextLine(args("input")))
    .flatMap { line => line.split("""\s+""") }
    .groupBy { word => word }
    .size
    .write(TypedTsv(args("output")))
```

### scald.rb

The `scald.rb` script in the `scripts/` directory is a handy script that makes it easy to run jobs in both local mode or on a remote Hadoop cluster. It handles simple command-line parsing, and copies over necessary JAR files when running remote jobs.

If you're running many Scalding jobs, it can be useful to add `scald.rb` to your path, so that you don't need to provide the absolute pathname every time. One way of doing this is via (something like):

    ln -s scripts/scald.rb $HOME/bin/

This creates a symlink to the `scald.rb` script in your `$HOME/bin/` directory (which should already be included in your PATH).

See [[scald.rb]] for more information, including instructions on how to set up the script to run jobs remotely.

## Next Steps

You now know the basics of Scalding! To learn more, check out the following resources:

* REPL Example: Try the [Alice in Wonderland walkthrough](https://gist.github.com/johnynek/a47699caa62f4f38a3e2) which shows how to use Scalding step by step to learn about the book's text.
* [tutorial/](https://github.com/twitter/scalding/tree/master/tutorial): this folder contains an introductory series of runnable jobs.
* [[API Reference]]: includes code snippets explaining different kinds of Scalding functions (e.g., map, filter, project, groupBy, join) and much more.
* [[Matrix API Reference]]: the API reference for the Type-safe Matrix library
* [Cookbook](https://github.com/willf/scalding_cookbook): Short recipes for common tasks.
