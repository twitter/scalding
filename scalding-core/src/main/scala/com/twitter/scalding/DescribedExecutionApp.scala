package com.twitter.scalding

object ArgOptions extends Enumeration {
  type ArgOptions = Value
  val RequiredArg, OptionalArg, ListArg, BooleanArg = Value
}
import ArgOptions._

case class DescribedArg(key: String, description: String, argOption: ArgOptions)

case class ArgHelp(describedArgs: Seq[DescribedArg]) {
  def argString: String = {
    describedArgs.foldLeft("") { case (str, DescribedArg(key, description, opt)) =>
      val msg = opt match {
        case RequiredArg => s"--$key VALUE "
        case OptionalArg => s"[--$key VALUE] "
        case ListArg => s"[--$key VALUE VALUE2] "
        case BooleanArg => s"[--$key] "
      }
      str + msg
    } + "[--help]"
  }

  def help: String = {
    describedArgs.foldLeft("") { case (str, DescribedArg(key, description, opt)) =>
      str + s"--$key($opt) :: $description \n"
    } + "--help :: Show this help message."
  }
}

/**
  * Extending this Trait allows for descriptions to be added to Jobs
  * If you run a job with --help the descriptions are output
  */
trait DescribedExecutionApp { this: ExecutionApp =>

  def descriptions: List[DescribedArg]

  def describedJob(args: Args): Execution[Unit]

  def job = {
    Execution.getArgs.flatMap{args =>
      if(args.boolean("help")){
        val helper = ArgHelp(descriptions)
        val top = "\n###########################################################################\n\n"
        val usage = s"Command Line Args :: ${helper.argString}\n\n\n"
        val help = helper.help
        val bottom = "\n\n###########################################################################\n"
        println(top + usage + help + bottom)
        Execution.unit
      } else {
        describedJob(args)
      }
    }
  }
}
