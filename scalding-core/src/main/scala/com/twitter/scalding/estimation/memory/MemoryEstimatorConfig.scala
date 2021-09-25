package com.twitter.scalding.estimation.memory

import org.apache.hadoop.mapred.JobConf

object MemoryEstimatorConfig {
  /** Output param: what the original job map memory was. */
  val originalMapMemory = "scalding.map.memory.estimator.original"

  /** Output param: what the original job reduce memory was. */
  val originalReduceMemory = "scalding.reduce.memory.estimator.original"

  /**
   * Value of alpha for exponential smoothing.
   * Lower values ensure more smoothing and less importance to newer data
   * Higher values provide lesser smoothing and more importance to newer data
   */
  val alphaKey = "scalding.memory.estimator.alpha"

  /** Indicates how much to scale the memory estimate after it's calculated */
  val memoryScaleFactor = "scalding.memory.estimator.scale.factor"

  val XmxToMemoryScaleFactorKey = "scalding.memory.estimator.xmx.scale.factor"

  val maxContainerMemoryKey = "scalding.memory.estimator.container.max"

  val minContainerMemoryKey = "scalding.memory.estimator.container.min"

  /** yarn allocates in increments. So we might as well round up our container ask **/
  val yarnSchedulerIncrementAllocationMB = "yarn.scheduler.increment-allocation-mb"

  /** Maximum number of history items to use for memory estimation. */
  val maxHistoryKey = "scalding.memory.estimator.max.history"

  def getMaxContainerMemory(conf: JobConf): Long = conf.getLong(maxContainerMemoryKey, 8 * 1024)

  def getMinContainerMemory(conf: JobConf): Long = conf.getLong(minContainerMemoryKey, 1 * 1024)

  def getAlpha(conf: JobConf): Double = conf.getDouble(alphaKey, 1.0)

  def getScaleFactor(conf: JobConf): Double = conf.getDouble(memoryScaleFactor, 1.2)

  def getXmxScaleFactor(conf: JobConf): Double = conf.getDouble(XmxToMemoryScaleFactorKey, 1.25)

  def getYarnSchedulerIncrement(conf: JobConf): Int = conf.getInt(yarnSchedulerIncrementAllocationMB, 512)

  def getMaxHistory(conf: JobConf): Int = conf.getInt(maxHistoryKey, 5)
}
