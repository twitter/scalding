package com.twitter.scalding.repl

import scala.tools.nsc.interpreter.ILoop

/**
 * A class providing Scalding specific commands for inclusion in the Scalding REPL.
 */
class ScaldingILoop
    extends ILoop {
  /**
   * Commands specific to the Scalding REPL. To define a new command use one of the following
   * factory methods:
   * - `LoopCommand.nullary` for commands that take no arguments
   * - `LoopCommand.cmd` for commands that take one string argument
   * - `LoopCommand.varargs` for commands that take multiple string arguments
   */
  private val scaldingCommands: List[LoopCommand] = List()

  /**
   * Change the shell prompt to read scalding&gt;
   *
   * @return a prompt string to use for this REPL.
   */
  override def prompt: String = "\nscalding> "

  /**
   * Gets the list of commands that this REPL supports.
   *
   * @return a list of the command supported by this REPL.
   */
  override def commands: List[LoopCommand] = super.commands ++ scaldingCommands
}
