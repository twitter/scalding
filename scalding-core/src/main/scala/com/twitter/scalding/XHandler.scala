package com.twitter.scalding

import cascading.flow.planner.PlannerException

/**
 * Provide handlers and mapping for exceptions
 * @param xMap - mapping as Map with Throwable class as key and String as value
 * @param dVal - default value for undefined keys in mapping
 */
class XHandler(xMap: Map[Class[_ <: Throwable], String], dVal: String) {

  def handlers: List[Throwable => Boolean] =
    xMap.keys.map(kCls => ((t: Throwable) => kCls == t.getClass)).toList

  def mapping: Class[_ <: Throwable] => String =
    xMap.withDefaultValue(dVal)
}

/**
 * Provide apply method for creating XHandlers with default or custom settings
 * and contain messages and mapping
 */
object RichXHandler {

  val Default = "Unknown type of throwable"

  val BinaryProblem = "GUESS: This may be a problem with the binary version of a dependency. " +
    "Check which versions of dependencies you're pulling in."

  val RequiredCascadingFabricNotInClassPath = "GUESS: Required Cascading fabric is not supplied in the classpath." +
    "Check which versions and variants of dependencies you're pulling in."

  val DataIsMissing = "GUESS: Data is missing from the path you provided."

  val RequireSinks = "GUESS: Cascading requires all sources to have final sinks on disk."

  val mapping: Map[Class[_ <: Throwable], String] = Map(
    classOf[ModeLoadException] -> RequiredCascadingFabricNotInClassPath,
    classOf[NoClassDefFoundError] -> BinaryProblem,
    classOf[AbstractMethodError] -> BinaryProblem,
    classOf[NoSuchMethodError] -> BinaryProblem,
    classOf[InvalidSourceException] -> DataIsMissing,
    classOf[PlannerException] -> RequireSinks)

  val gitHubUrl = "https://github.com/twitter/scalding/wiki/Common-Exceptions-and-possible-reasons#"

  @annotation.tailrec
  final def rootOf(t: Throwable): Throwable =
    t.getCause match {
      case null => t
      case cause => rootOf(cause)
    }

  @annotation.tailrec
  final def peelUntilMappable(t: Throwable): Class[_ <: Throwable] =
    (mapping.get(t.getClass), t.getCause) match {
      case (Some(diag), _) => t.getClass // we're going to find a mappable cause.
      case (None, null) => t.getClass // we're at the root. There won't be any cause
      case (None, cause) => peelUntilMappable(cause)
    }

  def createXUrl(t: Throwable): String =
    gitHubUrl + (peelUntilMappable(t).getName.replace(".", "").toLowerCase)

  def apply(xMap: Map[Class[_ <: Throwable], String] = mapping, dVal: String = Default) =
    new XHandler(xMap, dVal)

  def apply(t: Throwable): String =
    mapping.get(peelUntilMappable(t))
      .map(_ + "\n")
      .getOrElse("") +
      "If you know what exactly caused this error, please consider contributing to GitHub via following link.\n" +
      createXUrl(t)
}
