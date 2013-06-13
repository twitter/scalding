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
import java.util.UUID

class Tool extends hadoop.conf.Configured with hadoop.util.Tool {
  // This mutable state is not my favorite, but we are constrained by the Hadoop API:
  var rootJob : Option[(Args) => Job] = None

  //  Allows you to set the job for the Tool to run
  def setJobConstructor(jobc : (Args) => Job) {
    if(rootJob.isDefined) {
      sys.error("Job is already defined")
    }
    else {
      rootJob = Some(jobc)
    }
  }

  protected def getJob(args : Args) : Job = {
    if( rootJob.isDefined ) {
      rootJob.get.apply(args)
    }
    else if(args.positional.isEmpty) {
      sys.error("Usage: Tool <jobClass> --local|--hdfs [args...]")
    }
    else {
      val jobName = args.positional(0)
      // Remove the job name from the positional arguments:
      val nonJobNameArgs = args + ("" -> args.positional.tail)
      Job(jobName, nonJobNameArgs)
    }
  }

  // This both updates the jobConf with hadoop arguments
  // and returns all the non-hadoop arguments. Should be called once if
  // you want to process hadoop arguments (like -libjars).
  protected def nonHadoopArgsFrom(args : Array[String]) : Array[String] = {
    (new hadoop.util.GenericOptionsParser(getConf, args)).getRemainingArgs
  }

  def parseModeArgs(args : Array[String]) : (Mode, Args) = {
    val a = Args(nonHadoopArgsFrom(args))
    (Mode(a, getConf), a)
  }

  // Parse the hadoop args, and if job has not been set, instantiate the job
  def run(args : Array[String]) : Int = {
    val (mode, jobArgs) = parseModeArgs(args)
    // Connect mode with job Args
    run(getJob(Mode.putMode(mode, jobArgs)))
  }

  protected def run(job : Job) : Int = {

    val onlyPrintGraph = job.args.boolean("tool.graph")
    if (onlyPrintGraph) {
      // TODO use proper logging
      println("Only printing the job graph, NOT executing. Run without --tool.graph to execute the job")
    }

    /*
    * This is a tail recursive loop that runs all the
    * jobs spawned from this one
    */
    val jobName = job.getClass.getName
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
        println("writing DOT: " + thisDot)
        flow.writeDOT(thisDot)

        val thisStepsDot = jobName + cnt + "_steps.dot"
        println("writing Steps DOT: " + thisStepsDot)
        flow.writeStepsDOT(thisStepsDot)
        true
      }
      else {
        FlowStateMap.validateSources(j.flowDef, j.mode)
        //Block while the flow is running:
        j.run
      }
      FlowStateMap.clear(j.flowDef)
      //When we get here, the job is finished
      if(successful) {
        j.next match {
          case Some(nextj) => start(nextj, cnt + 1)
          case None => Unit
        }
      } else {
        throw new RuntimeException("Job failed to run: " + jobName +
          (if(cnt > 0) { " child: " + cnt.toString + ", class: " + j.getClass.getName }
          else { "" })
        )
      }
    }
    //start a counter to see how deep we recurse:
    start(job, 0)
    0
  }
}

object Tool {
  def main(args: Array[String]) {
    try {
      hadoop.util.ToolRunner.run(new hadoop.conf.Configuration, new Tool, args)
    } catch {
      case t: Throwable => {
         t.printStackTrace()
         if (RichXHandler().handlers.find(h => h(t)).isDefined) {
            println(RichXHandler.mapping(t.getClass))
         }
         //create the exception URL link in GitHub wiki
         val gitHubLink = RichXHandler.createXUrl(t)
         println("If you know what exactly caused this error, please consider contributing to GitHub via following link.\n"
          + gitHubLink)

      }
    }
  }
}
