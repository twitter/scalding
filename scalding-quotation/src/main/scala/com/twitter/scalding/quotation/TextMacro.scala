package com.twitter.scalding.quotation

import scala.reflect.macros.blackbox.Context

trait TextMacro {
  val c: Context
  import c.universe._

  def callText(method: TermName, params: List[Tree]): String =
    params.headOption.map(callText(method, _)).getOrElse(s"$method")

  def callText(method: TermName, firstParam: Tree): String =
    s"$method${paramsText(method, firstParam)}"

  /*
   * This should be something simple since Scala trees have the start and
   * end positions. However, there's a bug that makes the positions unreliable.
   * This method uses an ad-hoc parsing to get the text from the source file.
   */
  def paramsText(method: TermName, firstParam: Tree): String = {
    import c.universe._

    val fileContent = c.enclosingPosition.source.content.mkString

    /*
     * The start position of a tree isn't its actual start. It's necessary
     * to find the minimum start of the nested trees, which is reliable.
     */
    def start(t: Tree) = {
      def loop(t: List[Tree]): List[Position] =
        t.map(_.pos) ++ t.flatMap(t => loop(t.children))

      loop(List(t)).filter(_ != NoPosition).map(_.start).min
    }

    /*
     * From the first parameter start position, walk back until the method
     * call start and return the position immediately after the method name.
     */
    val content = {
      val reverseMethodName =
        method.decodedName.toString.reverse

      def paramsStartPosition(content: String, pos: Int): Int =
        if (content.startsWith(reverseMethodName) || content.isEmpty)
          pos
        else
          paramsStartPosition(content.drop(1), pos - 1)

      val firstParamStart = start(firstParam)

      val newStart =
        paramsStartPosition(
          fileContent.take(firstParamStart).reverse,
          firstParamStart)

      fileContent.drop(newStart).toList
    }

    val blockDelimiters =
      Map(
        '(' -> ')',
        '{' -> '}',
        '[' -> ']')

    /*
     * Reads the parameters block. It takes in consideration nested blocks like `map(v => { ... })` 
     */
    def readParams(chars: List[Char], open: List[Char], acc: List[Char] = Nil): (List[Char], List[Char]) =
      chars match {
        case Nil =>
          (acc, Nil)
        case head :: tail =>
          blockDelimiters.get(head) match {
            case Some(closing) =>
              val (block, rest) = readParams(tail, open :+ closing)
              readParams(rest, open, acc ++ (head +: block :+ closing))
            case None =>
              if (head != ' ' && (open.isEmpty || head == open.last))
                (acc, tail)
              else
                readParams(tail, open, acc :+ head)
          }
      }

    readParams(content, Nil)._1.mkString
  }
}