package com.twitter.scalding.repl

import cascading.flow.Flow
import cascading.pipe.Pipe
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.Mode

/**
 * Adds ability to run a pipe in the REPL.
 *
 * @param pipe to wrap.
 */
class ShellPipe(private val pipe: Pipe) {
  /**
   * Gets a job that can be used to run the data pipeline.
   *
   * @param args that should be used to construct the job.
   * @return a job that can be used to run the data pipeline.
   */
  private[scalding] def getJob(args: Args): Job = new Job(args) {
    // The job's constructor should evaluate to the pipe to run.
    pipe

    /**
     *  The flow definition used by this job, which should be the same as that used by the user
     *  when creating their pipe.
     */
    override implicit val flowDef = Implicits.flowDef

    /**
     * Obtains a configuration used when running the job.
     *
     * This overridden method uses the same configuration as a standard Scalding job,
     * but adds options specific to KijiScalding, including adding a jar containing compiled REPL
     * code to the distributed cache if the REPL is running.
     *
     * @param mode used to run the job (either local or hadoop).
     * @return the configuration that should be used to run the job.
     */
    override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
      // Use the configuration from Scalding Job as our base.
      val configuration = super.config(mode)

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
     * but clears the implicit flow definition defined in [[com.twitter.scalding.repl.Implicits]]
     * after the flow has been built from the flow definition. This allows additional pipelines
     * to be constructed and run after the pipeline encapsulated by this job.
     *
     * @param mode the mode in which the built flow will be run.
     * @return the flow created from the flow definition.
     */
    override def buildFlow(implicit mode: Mode): Flow[_] = {
      val flow = super.buildFlow(mode)
      Implicits.resetFlowDef()
      flow
    }
  }

  /**
   * Runs this pipe as a Scalding job.
   */
  def run() {
    getJob(new Args(Map())).run(Mode.mode)
  }
}
