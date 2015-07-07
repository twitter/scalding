package com.twitter.scalding.commons.source.lzotypedtext

import cascading.scheme.Scheme
import cascading.scheme.hadoop.{ TextDelimited => CHTextDelimited }
import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import com.twitter.elephantbird.cascading2.scheme.LzoTextDelimited
import com.twitter.scalding._
import com.twitter.scalding.source.typedtext._
import com.twitter.scalding.typed.TypedSink

object LzoTypedText {

  private val TAB = "\t"
  private val ONE = "\1"
  private val COMMA = ","

  /*
   * To use these, you will generally want to
   * import com.twitter.scalding.commons.source.typedtext._
   * to get the implicit TypedDescriptor.
   * Then use TypedText.lzoTzv[MyCaseClass]("path")
   */
  def lzoTsv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedLzoTypedText[T](path, TAB)
  def lzoOsv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedLzoTypedText[T](path, ONE)
  def lzoCsv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedLzoTypedText[T](path, COMMA)

  def hourlyLzoTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", TAB)
  }

  def hourlyLzoOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", ONE)
  }

  def hourlyLzoCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", COMMA)
  }

  def dailyLzoTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + "/*", TAB)
  }

  def dailyLzoOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + "/*", ONE)
  }

  def dailyLzoCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + "/*", COMMA)
  }

  def dailyPrefixSuffixLzoOsv[T](prefix: String, suffix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    require(suffix.head == '/', "suffix should include a preceding /")
    new TimePathLzoTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + suffix + "/*", ONE)
  }

}

trait LzoTypedTextDelimited[T] extends TypedTextDelimited[T] with LocalTapSource {
  override def hdfsScheme =
    HadoopSchemeInstance(new LzoTextDelimited(typeDescriptor.fields, false, false,
      separator, strict, null /* quote */ ,
      typeDescriptor.fields.getTypesClasses, safe).asInstanceOf[Scheme[_, _, _, _, _]])
}

class TimePathLzoTypedText[T](path: String, sep: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends TimePathedSource(path, dr, DateOps.UTC) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class MostRecentLzoTypedText[T](path: String, sep: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends MostRecentGoodSource(path, dr, DateOps.UTC) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class FixedLzoTypedText[T](path: String, sep: String)(implicit td: TypeDescriptor[T])
  extends FixedPathSource(path) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

