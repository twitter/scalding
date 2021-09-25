package com.twitter.scalding.quotation

import java.io.File

/**
 * Meta information about a method call.
 */
case class Quoted(position: Source, text: Option[String], projections: Projections) {
  override def toString = s"$position ${text.getOrElse("")}"
}

object Quoted {
  import language.experimental.macros
  implicit def method: Quoted = macro QuotedMacro.method

  private[scalding] def internal: Quoted = macro QuotedMacro.internal

  def function[T1, U](f: T1 => U): Function1[T1, U] with QuotedFunction = macro QuotedMacro.function
  def function[T1, T2, U](f: (T1, T2) => U): Function2[T1, T2, U] with QuotedFunction = macro QuotedMacro.function
  def function[T1, T2, T3, U](f: (T1, T2, T3) => U): Function3[T1, T2, T3, U] with QuotedFunction = macro QuotedMacro.function
  def function[T1, T2, T3, T4, U](f: (T1, T2, T3, T4) => U): Function4[T1, T2, T3, T4, U] with QuotedFunction = macro QuotedMacro.function
  def function[T1, T2, T3, T4, T5, U](f: (T1, T2, T3, T4, T5) => U): Function5[T1, T2, T3, T4, T5, U] with QuotedFunction = macro QuotedMacro.function
}

case class Source(path: String, line: Int) {
  def classFile = path.split(File.separator).last
  override def toString = s"$classFile:$line"
}

trait QuotedFunction {
  def quoted: Quoted
}
