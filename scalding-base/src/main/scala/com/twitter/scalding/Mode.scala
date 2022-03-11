package com.twitter.scalding

trait Mode extends java.io.Serializable {

  /**
   * Make the Execution.Writer for this platform
   */
  def newWriter(): Execution.Writer
}

object Mode {

  /**
   * This is a Args and a Mode together. It is used purely as a work-around for the fact that Job only accepts
   * an Args object, but needs a Mode inside.
   */
  private class ArgsWithMode(argsMap: Map[String, List[String]], val mode: Mode) extends Args(argsMap) {
    override def +(keyvals: (String, Iterable[String])): Args =
      new ArgsWithMode(super.+(keyvals).m, mode)
  }

  /** Attach a mode to these Args and return the new Args */
  def putMode(mode: Mode, args: Args): Args = new ArgsWithMode(args.m, mode)

  /** Get a Mode if this Args was the result of a putMode */
  def getMode(args: Args): Option[Mode] = args match {
    case withMode: ArgsWithMode => Some(withMode.mode)
    case _                      => None
  }
}

case class ModeException(message: String) extends RuntimeException(message)
case class ModeLoadException(message: String, origin: ClassNotFoundException) extends RuntimeException(origin)