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

import cascading.flow.hadoop.HadoopFlow
import cascading.flow.planner.BaseFlowStep

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.{ GenericOptionsParser, Tool => HTool, ToolRunner }

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class Tool extends Configured with HTool {
  // This mutable state is not my favorite, but we are constrained by the Hadoop API:
  var rootJob: Option[(Args) => Job] = None

  //  Allows you to set the job for the Tool to run
  def setJobConstructor(jobc: (Args) => Job): Unit = {
    if (rootJob.isDefined) {
      sys.error("Job is already defined")
    } else {
      rootJob = Some(jobc)
    }
  }

  protected def getJob(args: Args): Job = rootJob match {
    case Some(job) => job(args)
    case None if args.positional.isEmpty =>
      throw ArgsException("Usage: Tool <jobClass> --local|--hdfs [args...]")
    case None => // has at least one arg
      val jobName = args.positional.head
      // Remove the job name from the positional arguments:
      val nonJobNameArgs = args + ("" -> args.positional.tail)
      Job(jobName, nonJobNameArgs)
  }

  // This both updates the jobConf with hadoop arguments
  // and returns all the non-hadoop arguments. Should be called once if
  // you want to process hadoop arguments (like -libjars).
  protected def nonHadoopArgsFrom(args: Array[String]): Array[String] = {
    (new GenericOptionsParser(getConf, args)).getRemainingArgs
  }

  def parseModeArgs(args: Array[String]): (Mode, Args) = {
    val a = Args(nonHadoopArgsFrom(args))
    (Mode(a, getConf), a)
  }

  // Parse the hadoop args, and if job has not been set, instantiate the job
  def run(args: Array[String]): Int = {
    val (mode, jobArgs) = parseModeArgs(args)
    // Connect mode with job Args
    run(getJob(Mode.putMode(mode, jobArgs)))
  }

  protected def run(job: Job): Int = {

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
    def start(j: Job, cnt: Int): Unit = {
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

        /* We add descriptions if they exist to the stepName so it appears in the .dot file */
        flow match {
          case hadoopFlow: HadoopFlow =>
            val flowSteps = hadoopFlow.getFlowSteps.asScala
            flowSteps.foreach(step => {
              val baseFlowStep: BaseFlowStep[JobConf] = step.asInstanceOf[BaseFlowStep[JobConf]]
              val descriptions = baseFlowStep.getConfig.get(Config.StepDescriptions, "")
              if (!descriptions.isEmpty) {
                val stepXofYData = """\(\d+/\d+\)""".r.findFirstIn(baseFlowStep.getName).getOrElse("")
                // Reflection is only temporary.  Latest cascading has setName public: https://github.com/cwensel/cascading/commit/487a6e9ef#diff-0feab84bc8832b2a39312dbd208e3e69L175
                // https://github.com/twitter/scalding/issues/1294
                val x = classOf[BaseFlowStep[JobConf]].getDeclaredMethod("setName", classOf[String])
                x.setAccessible(true)
                x.invoke(step, "%s %s".format(stepXofYData, descriptions))
              }
            })
          case _ => // descriptions not yet supported in other modes
        }

        flow.writeDOT(thisDot)

        val thisStepsDot = jobName + cnt + "_steps.dot"
        println("writing Steps DOT: " + thisStepsDot)
        flow.writeStepsDOT(thisStepsDot)
        true
      } else {
        j.validate()
        j.run()
      }
      j.clear()
      //When we get here, the job is finished
      if (successful) {
        j.next match {
          case Some(nextj) => start(nextj, cnt + 1)
          case None => ()
        }
      } else {
        throw new RuntimeException("Job failed to run: " + jobName +
          (if (cnt > 0) { " child: " + cnt.toString + ", class: " + j.getClass.getName }
          else { "" }))
      }
    }
    //start a counter to see how deep we recurse:
    start(job, 0)
    0
  }
}

object Tool {
  def main(args: Array[String]): Unit = {
    try {
      ToolRunner.run(new JobConf, new Tool, ExpandLibJarsGlobs(args))
    } catch {
      case t: Throwable => {
        //re-throw the exception with extra info
        throw new Throwable(RichXHandler(t), t)
      }
    }
  }
}
