---
layout: page
title:  "FAQ"
section: "faq"
position: 3
---

# Frequently Asked Questions

Feel free to add new questions and to ping [@Scalding](http://twitter.com/scalding) for an answer.

# Running Scalding

### Who actually uses Scalding?

Twitter uses it in production all over the place!

Check out our [Powered By](powered_by.html) page for more examples.

### I'm having trouble with scald.rb, and I just want to run jars in my own system:

See this [conversation on Twitter](https://twitter.com/Joolz/status/264834261549457409).

### Can Scalding be run on Amazon's Elastic MapReduce?

Yes! See the [cascading-user group discussion](https://groups.google.com/forum/?fromgroups#!topic/cascading-user/5RfJa8n1JPo).  We would like to see someone prepare a patch for scald.rb to handle submission of scalding jobs to EMR.

### Scalding complains when I use a [TimePathedSource](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/FileSource.scala#L213) and some of the data is missing. How can I ignore that error?

Pass the option `--tool.partialok` to your job and it will ignore any missing data. It's safer to work around by either filling with place-holder empty files, or writing sources thatxb will skip known-missing dates. Using that option by default is very dangerous.

### I receive this error when running `sbt update`: <tt>Error occurred during initialization of VM. Incompatible minimum and maximum heap sizes specified</tt>

In your sbt script, set `local min=$(( $mem / 2 ))`

# Writing Jobs

### How do I make simple records for use in my scalding job?

We recommend cases classes **defined outside of your Job**. Case classes defined inside your job capture an $outer member variable that references the job that is wasteful for serialization. If you are having stack overflows during case class serialization this is likely your problem. If you have a use case this doesn't cover, email the cascading-user list or mention [@scalding](http://twitter.com/scalding). Dealing with serialization issues well in systems like Hadoop is tricky, and we're still improving our approaches.

See the [discussion on cascading-user](https://groups.google.com/forum/?fromgroups#!topic/cascading-user/kjpohwyC03Y).

### How do I pass parameters to my hadoop job (number of reducers , memory options , etc.) ?

```
hadoop jar myjar \
com.twitter.scalding.Tool \
-D mapred.output.compress=false  \
-D mapred.child.java.opts=-Xmx2048m \
-D mapred.reduce.tasks=20 \
com.class.myclass \
--hdfs \
--input $input \
--output $output
```

### How do I access the jobConf?

If you want to update the jobConf in your job, the way to do it is to override the config method in Job:

https://github.com/twitter/scalding/blob/cee3bb99ebb00db9622c387bee0b2718ab9cea61/scalding-core/src/main/scala/com/twitter/scalding/Job.scala#L163

If you really want to just read from the jobConf, you can do it with code like:

```scala
implicitly[Mode] match {
  case Hdfs(_, configuration) => {
    // use the configuration which is an instance of Configuration
  }
  case _ => error("Not running on Hadoop! (maybe cascading local mode?)")
}
```

See this discussion: https://groups.google.com/forum/?fromgroups=#!topic/cascading-user/YppTLebWds8

### How do I append my parameters to jobConf?

```scala
class WordCountJob(args : Args) extends Job(args) {

// Prior to 0.9.0 we need the mode, after 0.9.0 mode is a def on Job.
override def config(implicit m: Mode): Map[AnyRef,AnyRef] = {
   super.config ++ Map ("my.job.name" -> "my new job name")

  }
```

### What if I have more than 22 fields in my data-set?

**TODO: this answer refers to the DEPRECATED Fields API.**

Many of the examples (e.g. in the `tutorial/` directory) show that the fields argument is specified as a Scala Tuple when reading a delimited file. However Scala Tuples are currently limited to a maximum of 22 elements. To read-in a data-set with more than 22 fields, you can use a List of Symbols as fields specifier. E.g.

```scala
val mySchema = List('first, 'last, 'phone, 'age, 'country)

val input = Csv("/path/to/file.txt", separator = ",", fields = mySchema)
val output = TextLine("/path/to/out.txt")
input.read
     .project('age, 'country)
     .write(Tsv(output))
```

Another way to specify fields is using Scala Enumerations, which is available in the `develop` branch (as of Apr 2, 2013), as demonstrated in [Tutorial 6](https://github.com/twitter/scalding/blob/develop/tutorial/Tutorial6.scala):

```scala
object Schema extends Enumeration {
   val first, last, phone, age, country = Value // arbitrary number of fields
}

import Schema._

Csv("tutorial/data/phones.txt", separator = " ", fields = Schema)
  .read
  .project(first,age)
  .write(Tsv("tutorial/data/output6.tsv"))
```

### How do I increase the spill threshold?

The spilling is controlled with the same hadoop option as cascading:

```
-Dcascading.spill.list.threshold=1000000
```

Would keep 1 million items in memory.

The rule of thumb is use as much as you can without getting OOM.

### How do I increase the AggregateBy threshold value?

You can't set a default for AggregateBy, you need to set it in each reducer by calling spillThreshold function on GroupBuilder.
https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/GroupBuilder.scala#L97

### Q. My Hadoop job is erroring out with AbstractMethodError or IncompatibleClassChangeError.

A. If your job has dependencies that clash with Hadoop's, Hadoop can replace your version of a library (like log4j or ASM) with its own native version. You can fix this with an environment flag that makes sure that your jars show up on the classpath before Hadoop's. Set these environment variables:

    bash
    export HADOOP_CLASSPATH=<your_jar_file>
    export HADOOP_USER_CLASSPATH_FIRST=true

### Q. I'm getting a NotSerializableException on Hadoop job submission.

A. All fields in Job get serialized and sent to Hadoop. Your job contains an
object that is not serializable, even with Kryo. This issue may exhibit itself
as other exceptions, such as `InvocationTargetException`, `KryoException`, or
`IllegalAccessException`. What all these potential exceptions have in common
is being related to serialization failures during Hadoop job submission.

First, try to figure out which object is causing the problem.

For a better stacktrace than the usual opaque dump, try submitting your job again with the `extendedDebugInfo` flag set:

    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop <your-commands>

You should see a much larger stacktrace, with many entries like this:

```
    - field (class "com.twitter.scalding.MapsideReduce", name: "commutativeSemigroup", type: "interface com.twitter.algebird.Semigroup")
    - object (class "com.twitter.scalding.MapsideReduce", MapsideReduce[decl:'key', 'value'])
    - field (class "cascading.pipe.Operator", name: "operation", type: "interface cascading.operation.Operation")
    - object (class "cascading.pipe.Each", Each(_pipe_2*_pipe_3)[MapsideReduce[decl:'key', 'value']])
    - field (class "org.jgrapht.graph.IntrusiveEdge", name: "target", type: "class java.lang.Object")
    - object (class "org.jgrapht.graph.IntrusiveEdge", org.jgrapht.graph.IntrusiveEdge@6ed95e60)
    - custom writeObject data (class "java.util.HashMap")
    - object (class "java.util.LinkedHashMap", {[{?}:UNKNOWN]
[{?}:UNKNOWN]=org.jgrapht.graph.IntrusiveEdge@6ce4ece3, [{2}:0:1]
```

Typically, if you start reading from the bottom of these entries upward, the first familiar class you see will be the object that's being unexpectedly serialized and causing you issues. In this case, the error was with Scalding's =MapsideReduce= class.

Once you know which object is causing the problem, try one of the following remedies:

1. Put the object in a lazy val

2. Move it into a companion object, which will not be serialized.

3. If the item is only needed at submission, but not on the Mappers/Reducers, make it `@transient`.

If you see a common case we overlooked, let us know. Some common issues are inner classes to the Job (don't do that), Logger objects (don't put those in the job, put them in a companion), and some mutable Guava objects have given us trouble (we'd love to see this ticket closed: https://github.com/twitter/chill/issues/66 )

# Issues with Testing

### How do I get my tests working with Spec2?

from [Alex Dean, @alexatkeplar](https://twitter.com/alexatkeplar)

The problem was in how I was defining my tests. For Scalding, your Specs2 tests must look like this:
```scala
"A job which trys to do blah" should {
  <<RUN JOB>>
  "successfully do blah" in {
    expected.blah must_== actual.blah
  }
}
```

My problem was that my tests looked like this:

```scala
"A job which trys to do blah" should {
  "successfully do blah" in {
    <<RUN JOB>>
    expected.blah must_== actual.blah
  }
}
```
In other words, running the job was inside the `in {}`. For some reason, this was leading to multiple jobs running at the same time and conflicting with each others' output.

If anyone is interested, the diff which fixed my tests is here: https://github.com/snowplow/snowplow/commit/792ed2f9082b871ecedcf36956427a2f0935588c

### How can I work with HBase with scalding?

See the [Scalding and HBase](cookbook/hbase.html) page in the cookbook.

# Issues with SBT

Q) What version of SBT do I need? (It'd be great to capture the actual error that happens when you use the wrong version)

A) Get SBT 0.12.2. If you're having an older version of SBT, you can update it by typing in command line:

brew update;
brew unlink sbt;
brew install sbt

Q) What happens if I get OutOfMemoryErrors when running "sbt assembly"?

A) Create ~/.sbtconfig with these options:

```
SBT_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:PermSize=256M -XX:MaxPermSize=512M"
```

Q) What should I do if I get "value compare is not a member of object Integer" when running "./sbt compile"?

A) You're probably using Java 6 instead of Java 7.  You can specify which version of Java SBT should use by passing it the `-java-home` option.  For example, on a Mac you're SBT command might look something like:

```
./sbt -java-home /Library/Java/JavaVirtualMachines/<insert folder name of desired JVM version>/Contents/Home/
```

# Contributing code

### Do you accept pull requests?

Yes! By requesting a pull, you are agreeing to license your code under the same license as Scalding.

### To which branch do I make my pull request?

[develop](https://github.com/twitter/scalding/tree/develop)
