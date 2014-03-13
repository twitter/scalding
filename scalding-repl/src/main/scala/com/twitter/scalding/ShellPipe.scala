/*  Copyright 2013 Twitter, inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twitter.scalding

import cascading.flow.Flow
import cascading.pipe.Pipe

/**
 * Adds ability to run a pipe in the REPL.
 *
 * @param pipe to wrap.
 */
class ShellObj[T](obj: T) {
  /**
   * Gets a job that can be used to run the data pipeline.
   *
   * @param args that should be used to construct the job.
   * @return a job that can be used to run the data pipeline.
   */
  private[scalding] def getJob(args: Args, inmode: Mode): Job = new Job(args) {
    /**
     *  The flow definition used by this job, which should be the same as that used by the user
     *  when creating their pipe.
     */
    override val flowDef = ReplImplicits.flowDef

    override def mode = inmode

    /**
     * Obtains a configuration used when running the job.
     *
     * This overridden method uses the same configuration as a standard Scalding job,
     * but adds options specific to KijiScalding, including adding a jar containing compiled REPL
     * code to the distributed cache if the REPL is running.
     *
     * @return the configuration that should be used to run the job.
     */
    override def config: Map[AnyRef, AnyRef] = {
      // Use the configuration from Scalding Job as our base.
      val configuration: Map[AnyRef, AnyRef] = super.config

      /** Appends a comma to the end of a string. */
      def appendComma(str: Any): String = str.toString + ","

      // If the REPL is running, we should add tmpjars passed in from the command line,
      // and a jar of REPL code, to the distributed cache of jobs run through the REPL.
      val replCodeJar = ScaldingShell.createReplCodeJar()
      val tmpJarsConfig: Map[String, String] =
          if (replCodeJar.isDefined) {
            Map("tmpjars" -> {
              // Use tmpjars already in the configuration.
              configuration
                  .get("tmpjars")
                  .map(appendComma)
                  .getOrElse("") +
                  // And a jar of code compiled by the REPL.
                  "file://" + replCodeJar.get.getAbsolutePath
            })
          } else {
            // No need to add the tmpjars to the configuration
            Map()
          }

      configuration ++ tmpJarsConfig
    }

    /**
     * Builds a flow from the flow definition used when creating the pipeline run by this job.
     *
     * This overridden method operates the same as that of the super class,
     * but clears the implicit flow definition defined in [[com.twitter.scalding.ReplImplicits]]
     * after the flow has been built from the flow definition. This allows additional pipelines
     * to be constructed and run after the pipeline encapsulated by this job.
     *
     * @return the flow created from the flow definition.
     */
    override def buildFlow: Flow[_] = {
      val flow = super.buildFlow
      ReplImplicits.resetFlowDef()
      flow
    }
  }

  /**
   * Runs this pipe as a Scalding job.
   */
  def run() {
    val args = new Args(Map())
    getJob(args, ReplImplicits.mode).run
  }

  def toList[R](implicit ev: T <:< TypedPipe[R], manifest: Manifest[R]): List[R] = {
    import ReplImplicits._
    ev(obj).toPipe("el").write(Tsv("item"))
    run()
    TypedTsv[R]("item").toIterator.toList
  }
}

