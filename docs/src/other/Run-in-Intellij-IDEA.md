# Run in IntelliJ Idea

You can run your job from IDEA locally. Run -> Edit configurations -> New Application

| Option | Value |
|---|---|
Main class | com.twitter.scalding.Tool
VM options | -XX:MaxPermSize=512M -Xmx1024M
Program arguments | job.class.name --hdfs --param1 value --param2 value --input local-path
Working directory | ~/projects/scalding-jobs
Use classpath of module | first-party

Note that --output is optional usually. When not specified we write to NullSource which prints to stdout.

Here is sample scalding job:
```scala
package com.sample

import com.twitter.scalding._
import com.twitter.scalding.Tsv

class SampleJob(args: Args) extends Job(args) {
  val input = args("input")
  val output = args.getOrElse("output", null)

  val results = Tsv(input).read
  //todo do something here

  if (output != null)
    results.write(Tsv(output))
  else
    results.debug.write(NullSource)
}
```
