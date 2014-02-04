package com.twitter.scalding

import cascading.cascade.CascadeConnector
import cascading.cascade.Cascade

abstract class CascadeJob(args: Args) extends Job(args) {

  def jobs: Seq[Job]

  override def run : Boolean = {
    runCascade.getCascadeStats().isSuccessful()
  }

  def runCascade: Cascade = {
    val flows = jobs.map { _.buildFlow }
    val cascade = new CascadeConnector().connect(flows: _*)
    preProcessCascade(cascade)
    cascade.complete()
    postProcessCascade(cascade)
    cascade
  }

  override def validate {
    jobs.foreach { _.validate }
  }

  /*
   * Good for printing a dot file, setting the flow skip strategy, etc
   */
  def preProcessCascade(cascade: Cascade) = { }

  /*
   * Good for checking the cascade stats
   */
  def postProcessCascade(cascade: Cascade) = { }

}
