package com.twitter.scalding

sealed abstract class DescribedArg {
  def key: String
  def description: String
}

case class RequiredArg(key: String, description: String) extends DescribedArg
case class OptionalArg(key: String, description: String) extends DescribedArg
case class ListArg(key: String, description: String) extends DescribedArg
case class BooleanArg(key: String, description: String) extends DescribedArg

case class ArgHelp(describedArgs: Seq[DescribedArg]) {
  def argString: String = {
    describedArgs.foldLeft("") { case (str, describedArg) =>
      val msg = describedArg match {
        case RequiredArg(key, _) => s"--$key VALUE "
        case OptionalArg(key, _) => s"[--$key VALUE] "
        case ListArg(key, _) => s"[--$key VALUE VALUE2] "
        case BooleanArg(key, _) => s"[--$key] "
      }
      str + msg
    } + "[--help]"
  }

  def help: String = {
    describedArgs.foldLeft("") { case (str, describedArg) =>
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
