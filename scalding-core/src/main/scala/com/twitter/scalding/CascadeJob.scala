package com.twitter.scalding

import cascading.cascade.CascadeConnector
import cascading.cascade.Cascade

abstract class CascadeJob(args: Args) extends Job(args) {

  def jobs: Seq[Job]

  override def run : Boolean = {
    val flows = jobs.map { _.buildFlow }

    val cascade = new CascadeConnector().connect(flows: _*)
    preProcessCascade(cascade)
    cascade.complete()
    Stats.setCascadeStats(cascade.getCascadeStats)
    postProcessCascade(cascade)
    cascade.getCascadeStats().isSuccessful()
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
