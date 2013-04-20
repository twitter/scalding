package com.twitter.scalding

/**
 * Provide handlers and mapping for exceptions
 * @param m - mapping as Map with Throwable class as key and String as value
 * @param d - default value for undefined keys in mapping
 */
class XHandler(m: Map[Class[_ <: Throwable], String], d: String) {

  def handlers = m.keys.map(c => ((t: Throwable) => c == t.getClass)).toList

  def mapping: Class[_ <: Throwable] => String = m.withDefaultValue(d)

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

  def apply(m: Map[Class[_ <: Throwable], String] = mapping, d: String = Default) = new XHandler(m, d)
}
