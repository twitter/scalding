package com.twitter.scalding

object ArgOptions extends Enumeration {
  type ArgOptions = Value
  val RequiredArg, OptionalArg, ListArg, BooleanArg = Value
}

class DescriptionArgs(override val m: Map[String, List[String]]) extends Args(m){
  import ArgOptions._

  private val map = new scala.collection.mutable.HashMap[String, (String, ArgOptions)]()

  def isHelpState = boolean("help")

  def optionalDescription(key: String, description: String): Unit =
    map += key -> (description, OptionalArg)

  def requiredDescription(key: String, description: String): Unit =
    map += key -> (description, RequiredArg)

  def booleanDescription(key: String, description: String): Unit =
    map += key -> (description, BooleanArg)

  def listDescription(key: String, description: String): Unit =
    map += key -> (description, ListArg)


  def argString: String = {
    map.foldLeft("") { case (str, (key, (description, opt))) =>
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
    map.foldLeft("") { case (str, (key, (description, opt))) =>
      str + s"--$key($opt) :: $description \n"
    } + "--help :: Show this help message."
  }
}

/**
  * Extending this Trait allows for descriptions to be added to Jobs
  * If you run a job with --help the descriptions are output
  */
trait DescribedExecutionApp { this: ExecutionApp =>

  def descriptions(args: DescriptionArgs): Unit

  def describedJob(args: Args): Execution[Unit]

  def job = {
    Execution.getArgs.flatMap{args =>
      val descriptionArgs = new DescriptionArgs(args.m)
      descriptions(descriptionArgs)
      if(descriptionArgs.isHelpState){
        val top = "\n###########################################################################\n\n"
        val usage = s"Command Line Args :: ${descriptionArgs.argString}\n\n\n"
        val help = descriptionArgs.help
        val bottom = "\n\n###########################################################################\n"
        println(top + usage + help + bottom)
        Execution.unit
      } else {
        describedJob(descriptionArgs)
      }
    }
  }
}
