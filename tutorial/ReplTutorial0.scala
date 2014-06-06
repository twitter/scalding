/*
Copyright 2012 Twitter, Inc.

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
import com.twitter.scalding._

/**
Scalding REPL tutorial part 0.

This is Tutorial0 in REPL form - the simplest possible scalding job.

To test it, first make sure you've built the target/scalding-assembly-XXX.jar:
from the base directory type:
  sbt assembly

Now use the scald-repl.sh and run this job, redirecting it from stdin
  scripts/scald-repl.sh --local < tutorial/ReplTutorial0.scala

You can check the input:
  cat tutorial/data/hello.txt

And the output:
  cat tutorial/data/output0.txt

The output should look just like the input, but with line numbers.
**/

/**
Both input and output data sources are represented by instances of
com.twitter.scalding.Source.

Scalding comes with some basic source types like TextLine and Tsv.
There are also many twitter-specific types like MergedAdRequestSource.
**/

val input = TextLine("tutorial/data/hello.txt")
val output = TextLine("tutorial/data/output0.txt")

/**
You can then define a pipe that reads the source and writes to the sink.
**/
val pipe = input.read.write(output)

/**
And then run it! (But only once.)
**/
pipe.run

/**
Exit cleanly from the REPL
**/
exit
