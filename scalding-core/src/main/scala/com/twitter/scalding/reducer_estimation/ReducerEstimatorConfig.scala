package com.twitter.scalding.reducer_estimation

import org.apache.hadoop.mapred.JobConf

object ReducerEstimatorConfig {

  /** Output param: what the Reducer Estimator recommended, regardless of if it was used. */
  val estimatedNumReducers = "scalding.reducer.estimator.result"

  /**
   * Output param: same as estimatedNumReducers but with the cap specified by maxEstimatedReducersKey
   * applied. Can be used to determine whether a cap was applied to the estimated number of reducers
   * and potentially to trigger alerting / logging.
   */
  val cappedEstimatedNumReducersKey = "scalding.reducer.estimator.result.capped"

  /** Output param: what the original job config was. */
  val originalNumReducers = "scalding.reducer.estimator.original.mapred.reduce.tasks"

  /**
   * If we estimate more than this number of reducers,
   * we will use this number instead of the estimated value
   */
  val maxEstimatedReducersKey = "scalding.reducer.estimator.max.estimated.reducers"

  /* fairly arbitrary choice here -- you will probably want to configure this in your cluster defaults */
  val defaultMaxEstimatedReducers = 5000

  /** Maximum number of history items to use for reducer estimation. */
  val maxHistoryKey = "scalding.reducer.estimator.max.history"

  def getMaxHistory(conf: JobConf): Int = conf.getInt(maxHistoryKey, 1)
}
