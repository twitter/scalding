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
package com.twitter.scalding

import org.apache.hadoop
import cascading.tuple.Tuple
import collection.mutable.{ListBuffer, Buffer}
import scala.annotation.tailrec

class Tool extends hadoop.conf.Configured with hadoop.util.Tool {
  def run(args : Array[String]) : Int = {
    val config = getConf()
    val remainingArgs = (new hadoop.util.GenericOptionsParser(config, args)).getRemainingArgs

    if(remainingArgs.length < 2) {
      System.err.println("Usage: Tool <jobClass> --local|--hdfs [args...]")
      return 1
    }

    val mode = remainingArgs(1)
    val jobName = remainingArgs(0)
    val firstargs = Args(remainingArgs.tail.tail)
    //
    val strictSources = firstargs.boolean("tool.partialok") == false
    if (!strictSources) {
      println("[Scalding:INFO] using --tool.partialok. Missing log data won't cause errors.")
    }

    Mode.mode = mode match {
      case "--local" => Local(strictSources)
      case "--hdfs" => Hdfs(strictSources, config)
      case _ => {
        System.err.println("[ERROR] Mode must be one of --local or --hdfs, you provided '" + mode + "'")
        return 1
      }
    }

    val onlyPrintGraph = firstargs.boolean("tool.graph")
    if (onlyPrintGraph) {
      println("Only printing the job graph, NOT executing. Run without --tool.graph to execute the job")
    }

    /*
    * This is a tail recursive loop that runs all the
    * jobs spawned from this one
    */
    @tailrec
    def start(j : Job, cnt : Int) {
      val successful = if (onlyPrintGraph) {
        val flow = j.buildFlow
        /*
        * This just writes out the graph representing
        * all the cascading elements that are created for this
        * flow. Use graphviz to render it as a PDF.
        * The job is NOT run in this case.
        */
        val thisDot = jobName + cnt + ".dot"
        println("writing: " + thisDot)
        flow.writeDOT(thisDot)
        true
      }
      else {
        //Block while the flow is running:
        j.run
      }
      //When we get here, the job is finished
      if(successful) {
        j.next match {
          case Some(nextj) => start(nextj, cnt + 1)
          case None => Unit
        }
      } else {
        throw new RuntimeException("Job failed to run: " + jobName)
      }
    }
    //start a counter to see how deep we recurse:
    start(Job(jobName, firstargs), 0)
    return 0
  }
}

object Tool {
  def main(args : Array[String]) {
    hadoop.util.ToolRunner.run(new hadoop.conf.Configuration, new Tool, args);
  }
}
