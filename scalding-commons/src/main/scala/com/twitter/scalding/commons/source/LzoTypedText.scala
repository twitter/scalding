package com.twitter.scalding.commons.source

import cascading.scheme.Scheme
import cascading.scheme.hadoop.{ TextDelimited => CHTextDelimited }
import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import com.twitter.elephantbird.cascading3.scheme.LzoTextDelimited
import com.twitter.scalding._
import com.twitter.scalding.source.TypedTextDelimited
import com.twitter.scalding.source.TypedSep
import com.twitter.scalding.typed.TypedSink

object LzoTypedText {

  val TAB = TypedSep("\t")
  val ONE = TypedSep("\u0001")
  val COMMA = TypedSep(",")

  /*
   * To use these, you will generally want to
   * import com.twitter.scalding.commons.source.typedtext._
   * to get the implicit TypedDescriptor.
   * Then use TypedText.lzoTzv[MyCaseClass]("path")
   */
  def lzoTsv[T: TypeDescriptor](path: String*): TypedTextDelimited[T] = new FixedLzoTypedText[T](TAB, path: _*)
  def lzoOsv[T: TypeDescriptor](path: String*): TypedTextDelimited[T] = new FixedLzoTypedText[T](ONE, path: _*)
  def lzoCsv[T: TypeDescriptor](path: String*): TypedTextDelimited[T] = new FixedLzoTypedText[T](COMMA, path: _*)

  def hourlyLzoTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](TAB, prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*")
  }

  def hourlyLzoOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](ONE, prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*")
  }

  def hourlyLzoCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](COMMA, prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*")
  }

  def dailyLzoTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](TAB, prefix + TimePathedSource.YEAR_MONTH_DAY + "/*")
  }

  def dailyLzoOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](ONE, prefix + TimePathedSource.YEAR_MONTH_DAY + "/*")
  }

  def dailyLzoCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathLzoTypedText[T](COMMA, prefix + TimePathedSource.YEAR_MONTH_DAY + "/*")
  }

  def dailyPrefixSuffixLzoOsv[T](prefix: String, suffix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    require(suffix.head == '/', "suffix should include a preceding /")
    new TimePathLzoTypedText[T](ONE, prefix + TimePathedSource.YEAR_MONTH_DAY + suffix + "/*")
  }

}

trait LzoTypedTextDelimited[T] extends TypedTextDelimited[T] with LocalTapSource {
  override def hdfsScheme =
    HadoopSchemeInstance(new LzoTextDelimited(typeDescriptor.fields, false, false,
      separator.str, strict, null /* quote */ ,
      typeDescriptor.fields.getTypesClasses, safe).asInstanceOf[Scheme[_, _, _, _, _]])
}

class TimePathLzoTypedText[T](sep: TypedSep, path: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends TimePathedSource(path, dr, DateOps.UTC) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class MostRecentLzoTypedText[T](sep: TypedSep, path: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends MostRecentGoodSource(path, dr, DateOps.UTC) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class FixedLzoTypedText[T](sep: TypedSep, path: String*)(implicit td: TypeDescriptor[T])
  extends FixedPathSource(path: _*) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

