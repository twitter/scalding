package com.twitter.scalding

sealed trait DescribedArg {
  def key: String
  def description: String
}

final case class RequiredArg(key: String, description: String) extends DescribedArg
final case class OptionalArg(key: String, description: String) extends DescribedArg
final case class ListArg(key: String, description: String) extends DescribedArg
final case class BooleanArg(key: String, description: String) extends DescribedArg

class HelpException extends RuntimeException("User asked for help")
class DescriptionValidationException(msg: String) extends RuntimeException(msg)

trait ArgHelper {
  /**
   * Similar to describe but validate all args are described
   *
   * @param describedArgs List of Argument Descriptions
   * @param ex Input Execution
   * @return Output Execution
   */
  def validatedDescribe[T](describedArgs: Seq[DescribedArg], ex: Execution[T]): Execution[T] = {
    Execution.getArgs.flatMap { args =>
      validatedDescribe(describedArgs, args)
      ex
    }
  }

  /**
   * Describe a set of Args given Descriptions and validate all Args are described
   * @param describedArgs List of Argument Descriptions
   * @param args Job Arguments
   */
  def validatedDescribe(describedArgs: Seq[DescribedArg], args: Args): Unit = {
    describe(describedArgs, args)

    val describedKeys = describedArgs.map(_.key).toSet
    val missingKeys = args.m.keySet.filter(_.nonEmpty).diff(describedKeys)

    if (missingKeys.nonEmpty) {
      val msg = missingKeys.mkString(", ")
      throw new DescriptionValidationException(s"Must describe missing keys : $msg")
    }
  }

  /**
   * Describe the Arguments of this Execution.  By running --help the args will output
   * and the execution will end
   *
   * @param describedArgs List of Argument Descriptions
   * @param ex Input Execution
   * @return Output Execution
   */
  def describe[T](describedArgs: Seq[DescribedArg], ex: Execution[T]): Execution[T] = {
    Execution.getArgs.flatMap { args =>
      describe(describedArgs, args)
      ex
    }
  }

  /**
   * Describe a set of Args given Descriptions
   *
   * @param describedArgs List of Argument Descriptions
   * @param args Job Arguments
   */
  def describe(describedArgs: Seq[DescribedArg], args: Args): Unit =
    if (args.boolean("help")) helpRequest(describedArgs)
    else ()

  def helpRequest(describedArgs: Seq[DescribedArg]): Nothing = {
    val top = "\n###########################################################################\n\n"
    val usage = s"Command Line Args :: ${argString(describedArgs)}\n\n\n"
    val bottom = "\n\n###########################################################################\n"

    println(top + usage + help(describedArgs) + bottom)

    throw new HelpException()
  }

  /**
   * Command line arg string given the Described Args
   *
   * @param describedArgs List of Argument Descriptions
   * @return Command Line Parameters
   */
  private[this] def argString(describedArgs: Seq[DescribedArg]): String = {
    describedArgs.foldLeft("") {
      case (str, describedArg) =>
        val msg = describedArg match {
          case RequiredArg(key, _) => s"--$key VALUE "
          case OptionalArg(key, _) => s"[--$key VALUE] "
          case ListArg(key, _) => s"[--$key VALUE VALUE2] "
          case BooleanArg(key, _) => s"[--$key] "
        }
        str + msg
    } + "[--help]"
  }

  /**
   * More detailed help command for these described arguments
   *
   * @param describedArgs List of Argument Descriptions
   * @return Detailed Help for the Args
   */
  private[this] def help(describedArgs: Seq[DescribedArg]): String = {
    describedArgs.foldLeft("") {
      case (str, describedArg) =>
        val msg = describedArg match {
          case RequiredArg(key, description) => s"--$key(Required) :: $description \n"
          case OptionalArg(key, description) => s"--$key(Optional) :: $description \n"
          case ListArg(key, description) => s"--$key(List) :: $description \n"
          case BooleanArg(key, description) => s"--$key(Boolean) :: $description \n"
        }
        str + msg
    } + "--help :: Show this help message."
  }
}

object ArgHelp extends ArgHelper
