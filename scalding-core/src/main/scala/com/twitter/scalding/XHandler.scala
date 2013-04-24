package com.twitter.scalding

/**
 * Provide handlers and mapping for exceptions
 * @param xMap - mapping as Map with Throwable class as key and String as value
 * @param dVal - default value for undefined keys in mapping
 */
class XHandler(xMap: Map[Class[_ <: Throwable], String], dVal: String) {

  def handlers = xMap.keys.map(kCls => ((t: Throwable) => kCls == t.getClass)).toList

  def mapping: Class[_ <: Throwable] => String = xMap.withDefaultValue(dVal)

}


/**
 * Provide apply method for create XHandlers with default or custom settings
 * and contain messages and mapping
 */
object RichXHandler {

  val Default = "Unknown type of throwable"

  val BinaryProblem = "GUESS: This may be a problem with the binary version of a dependency." +
    "Check which versions of dependencies you're pulling in."

  val DataIsMissing = "TL;DR: Data is missing."

  val RequireAllSources = "TL;DR: Cascading requires all sources to have final outputs on disk."


  val mapping: Map[Class[_ <: Throwable], String] = Map(
    (classOf[NoClassDefFoundError] -> BinaryProblem),
    (classOf[AbstractMethodError] -> DataIsMissing),
    (classOf[IndexOutOfBoundsException] -> RequireAllSources)
  )

  def apply(xMap: Map[Class[_ <: Throwable], String] = mapping, dVal: String = Default) = new XHandler(xMap, dVal)
}
