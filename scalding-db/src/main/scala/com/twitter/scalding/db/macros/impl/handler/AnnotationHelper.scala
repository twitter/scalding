package com.twitter.scalding.db.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.util.{ Success, Failure }

import com.twitter.scalding.db.ColumnDefinition
import com.twitter.scalding.db.macros.impl.FieldName

private[handler] sealed trait SizeAnno
private[handler] case class WithSize(v: Int) extends SizeAnno
private[handler] case object WithoutSize extends SizeAnno

private[handler] sealed trait DateAnno
private[handler] case object WithDate extends DateAnno
private[handler] case object WithoutDate extends DateAnno

private[handler] sealed trait VarcharAnno
private[handler] case object WithVarchar extends VarcharAnno
private[handler] case object WithoutVarchar extends VarcharAnno

private[handler] sealed trait TextAnno
private[handler] case object WithText extends TextAnno
private[handler] case object WithoutText extends TextAnno

private[handler] abstract class AnnotationHelper {
  val ctx: Context
  val cfieldName: FieldName
  val cannotationInfo: List[(ctx.universe.Type, Option[Int])]
  import ctx.universe._

  def sizeAnnotation: scala.util.Try[(AnnotationHelper, SizeAnno)] =
    consume[SizeAnno](typeOf[com.twitter.scalding.db.macros.size])(_.flatten.map(o => WithSize(o)).getOrElse(WithoutSize))

  def textAnnotation: scala.util.Try[(AnnotationHelper, TextAnno)] =
    consume(typeOf[com.twitter.scalding.db.macros.text])(_.map(_ => WithText).getOrElse(WithoutText))

  def varcharAnnotation: scala.util.Try[(AnnotationHelper, VarcharAnno)] =
    consume(typeOf[com.twitter.scalding.db.macros.varchar])(_.map(_ => WithVarchar).getOrElse(WithoutVarchar))

  def dateAnnotation: scala.util.Try[(AnnotationHelper, DateAnno)] =
    consume(typeOf[com.twitter.scalding.db.macros.date])(_.map(_ => WithDate).getOrElse(WithoutDate))

  def consume[T](t: ctx.universe.Type)(fn: Option[Option[Int]] => T): scala.util.Try[(AnnotationHelper, T)] = {
    val (matchedAnnotations, remainingAnnotations) = cannotationInfo.partition {
      case (tpe, _) => tpe =:= t
    }

    val newHelper = new {
      val ctx: this.ctx.type = this.ctx
      val cfieldName = this.cfieldName
      val cannotationInfo: List[(this.ctx.universe.Type, Option[Int])] = remainingAnnotations
    } with AnnotationHelper

    matchedAnnotations match {
      case h :: Nil => Success((newHelper, fn(Some(h._2))))
      case h :: t => Failure(new Exception(s"Error more than one annotation when looking for $t"))
      case Nil => Success((newHelper, fn(None)))
    }
  }

  def validateFinished: scala.util.Try[Unit] = {
    if (cannotationInfo.isEmpty) {
      Success(())
    } else {
      val msg = s"""
        Finished consuming annotations for field ${cfieldName.toStr}, but have remaining annotations:
        ${cannotationInfo.map(_._1).mkString("\n")}
        """
      Failure(new Exception(msg))
    }
  }
}
