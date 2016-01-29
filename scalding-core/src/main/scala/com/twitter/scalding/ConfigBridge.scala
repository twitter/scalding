package com.twitter.scalding

import cascading.flow.FlowStep

import scala.collection.JavaConverters._

/**
 * Created by cchepelov on 13/01/16.
 */
object ConfigBridge {
  /**
   * This adapter handles the various vessel types that can be used to configure properties of a a
   * FlowStep[_<: Any] since Cascading 3.0
   */
  class FlowStepAdapter(flowStep: FlowStep[_ <: Any]) {
    def getConfigValue(key: String): String =
      flowStep.getConfig match {
        case conf: org.apache.hadoop.conf.Configuration => conf.get(key)
        case conf: org.apache.commons.configuration.Configuration => conf.getString(key)
        case conf: java.util.Properties => conf.getProperty(key)
        case _ => throw new NotImplementedError(s"unknown flowStep Config type ${flowStep.getConfig.getClass}")
      }

    def setConfigValue(name: String, value: String): FlowStep[_ <: Any] = {
      flowStep.getConfig match {
        case conf: org.apache.hadoop.conf.Configuration => conf.set(name, value)
        case conf: org.apache.commons.configuration.Configuration => conf.addProperty(name, value)
        case conf: java.util.Properties => conf.put(name, value)
        case _ => throw new NotImplementedError(s"unknown flowStep Config type ${flowStep.getConfig.getClass}")
      }
      flowStep
    }
  }

  def fromPlatform(anyConf: Any): Config = {
    anyConf match {
      /* NOTE: for now we always return a Config instance (actually, an anonymous realization of the Config trait)
        no matter what the underlying fabric/platform. Here would be a GREAT opportunity to return a specific Config
        implementation (notably, to deal with things like HadoopNumReducers* )
         */

      // use `conf.get` to force JobConf to evaluate expressions
      case conf: org.apache.hadoop.conf.Configuration => Config(conf.asScala.map { e => e.getKey -> conf.get(e.getKey) }.toMap)
      case conf: org.apache.commons.configuration.Configuration => Config(conf.getKeys.asScala.map { k => k.asInstanceOf[String] }.map { (k: String) => k -> conf.getString(k) }.toMap)
      case conf: java.util.Properties => Config(conf.asScala.map { e => e._1 -> conf.getProperty(e._1) }.toMap)
      case _ => throw new NotImplementedError(s"Can't get from 'hadoop' with configuration type ${anyConf.getClass}")
    }
  }

  implicit def toFlowStepPimpFromFlowStep(flowStep: FlowStep[_ <: Any]): FlowStepAdapter = new FlowStepAdapter(flowStep)
}