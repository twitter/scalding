package com.twitter.scalding.estimation.memory

import cascading.flow.{ Flow, FlowStep, FlowStepStrategy }
import com.twitter.algebird.Monoid
import com.twitter.scalding.estimation.{ Estimator, FallbackEstimatorMonoid, FlowStrategyInfo }
import com.twitter.scalding.{ Config, StringUtility }
import java.util.{ List => JList }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

object MemoryEstimatorStepStrategy extends FlowStepStrategy[JobConf] {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  implicit val estimatorMonoid: Monoid[Estimator[MemoryEstimate]] =
    new FallbackEstimatorMonoid[MemoryEstimate]

  /**
   * Make memory estimate, possibly overriding explicitly-set memory settings,
   * and save useful info (such as the original & estimate value of memory settings)
   * in JobConf for later consumption.
   *
   * Called by Cascading at the start of each job step.
   */
  final override def apply(
    flow: Flow[JobConf],
    preds: JList[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {

    if (skipMemoryEstimation(step)) {
      LOG.info(s"Skipping memory estimation as ${Config.MemoryEstimators} is not set ")
    } else {
      estimate(flow, preds.asScala, step)
    }
  }

  private[estimation] def skipMemoryEstimation(step: FlowStep[JobConf]): Boolean =
    step.getConfig.get(Config.MemoryEstimators, "").isEmpty

  private[estimation] def estimate(
    flow: Flow[JobConf],
    preds: Seq[FlowStep[JobConf]],
    step: FlowStep[JobConf]): Unit = {
    val conf = step.getConfig

    Option(conf.get(Config.MemoryEstimators)).foreach { clsNames =>

      val clsLoader = Thread.currentThread.getContextClassLoader

      val estimators = StringUtility.fastSplit(clsNames, ",")
        .map(clsLoader.loadClass(_).newInstance.asInstanceOf[Estimator[MemoryEstimate]])
      val combinedEstimator = Monoid.sum(estimators)

      val info = FlowStrategyInfo(flow, preds, step)

      // get memory estimate
      val memoryEstimate: Option[MemoryEstimate] = combinedEstimator.estimate(info)

      memoryEstimate match {
        case Some(MemoryEstimate(Some(mapMem), Some(reduceMem))) =>
          LOG.info(s"Overriding map memory to: $mapMem in Mb and reduce memory to: $reduceMem in Mb")
          setMemory(mapMem, (Config.MapJavaOpts, Config.MapMemory), conf)
          setMemory(reduceMem, (Config.ReduceJavaOpts, Config.ReduceMemory), conf)
        case Some(MemoryEstimate(Some(mapMem), _)) =>
          LOG.info(s"Overriding only map memory to: $mapMem in Mb")
          setMemory(mapMem, (Config.MapJavaOpts, Config.MapMemory), conf)
        case Some(MemoryEstimate(_, Some(reduceMem))) =>
          LOG.info(s"Overriding only reduce memory to: $reduceMem in Mb")
          setMemory(reduceMem, (Config.ReduceJavaOpts, Config.ReduceMemory), conf)
        case _ =>
          LOG.info("Memory estimators didn't calculate any value. Skipping setting memory overrides")
          // explicitly unset these as Cascading seems to set them to 1024M
          conf.unset(Config.MapMemory)
          conf.unset(Config.ReduceMemory)
      }
    }
  }

  private[estimation] def setMemory(memorySettings: (Long, Long), keys: (String, String), conf: JobConf): Unit = {
    val (xmxMemory, containerMemory) = memorySettings
    val (xmxKey, containerKey) = keys

    conf.setLong(containerKey, containerMemory)

    setXmxMemory(xmxKey, xmxMemory, conf)
  }

  private[estimation] def setXmxMemory(xmxKey: String, xmxMemory: Long, conf: JobConf): Unit = {
    val xmxOpts = conf.get(xmxKey, "")
    //remove existing xmx / xms
    val xmxOptsWithoutXm = xmxOpts.split(" ").filterNot(s => s.startsWith("-Xmx") || s.startsWith("-Xms")).mkString(" ")

    conf.set(xmxKey, xmxOptsWithoutXm + s" -Xmx${xmxMemory}m")
  }
}
