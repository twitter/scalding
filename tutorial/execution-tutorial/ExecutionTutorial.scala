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
package com.twitter.scalding.tutorial

import java.io._
import scala.util.{Failure, Success}

import com.twitter.scalding._

/**
Tutorial of using Execution

This tutorial gives an example of use Execution to do MapReduce word count.
Instead of writing the results in reducers, it writes the data at submitter node.

To test it, first build the assembly jar from root directory:
 ./sbt execution-tutorial/assembly

Run:
  scala -classpath  tutorial/execution-tutorial/target/execution-tutorial-assembly-0.17.0.jar \
    com.twitter.scalding.tutorial.MyExecJob --local \
    --input tutorial/data/hello.txt \
    --output tutorial/data/execution_output.txt
**/

object MyExecJob extends ExecutionApp {
  
  override def job = Execution.getConfig.flatMap { config =>
    val args = config.getArgs
    
    TypedPipe.from(TextLine(args("input")))
      .flatMap(_.split("\\s+"))
      .map((_, 1L))
      .sumByKey
      .toIterableExecution
      // toIterableExecution will materialize the outputs to submitter node when finish.
      // We can also write the outputs on HDFS via .writeExecution(TypedTsv(args("output")))
      .onComplete { t => t match {
        case Success(iter) => 
          val file = new PrintWriter(new File(args("output")))
          iter.foreach { case (k, v) =>
              file.write(s"$k\t$v\n")
          }
          file.close
        case Failure(e) => println("Error: " + e.toString)
        }
      }
      // use the result and map it to a Unit. Otherwise the onComplete call won't happen
      .unit
  }
}


