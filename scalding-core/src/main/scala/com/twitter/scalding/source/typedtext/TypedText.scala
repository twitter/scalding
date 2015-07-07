package com.twitter.scalding.source.typedtext

import cascading.scheme.Scheme
import cascading.scheme.hadoop.{ TextDelimited => CHTextDelimited }
import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import com.twitter.elephantbird.cascading2.scheme.LzoTextDelimited
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink

/**
 * This object gives you easy access to text formats (possibly LZO compressed) by
 * using a case class to describe the field names and types.
 */
object TypedText {

  private val TAB = "\t"
  private val ONE = "\1"
  private val COMMA = ","

  /*
   * To use these, you will generally want to
   * import com.twitter.scalding_internal.source.typedtext._
   * to get the implicit TypedDescriptor.
   * Then use TypedText.lzoTzv[MyCaseClass]("path")
   */
  def lzoTsv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedLzoTypedText[T](path, TAB)
  def lzoOsv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedLzoTypedText[T](path, ONE)
  def lzoCsv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedLzoTypedText[T](path, COMMA)
  def tsv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedTypedText[T](path, TAB)
  def osv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedTypedText[T](path, ONE)
  def csv[T: TypeDescriptor](path: String): TypedTextDelimited[T] = new FixedTypedText[T](path, COMMA)

  /**
   * Prefix might be "/logs/awesome"
   */
  def hourlyTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", TAB)
  }
  def hourlyOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", ONE)
  }
  def hourlyCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*", COMMA)
  }
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

  def dailyTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + "/*", TAB)
  }
  def dailyOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + "/*", ONE)
  }
  def dailyCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + "/*", COMMA)
  }
  def dailyPrefixSuffixOsv[T](prefix: String, suffix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    require(suffix.head == '/', "suffix should include a preceding /")
    new TimePathTypedText[T](prefix + TimePathedSource.YEAR_MONTH_DAY + suffix + "/*", ONE)
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

trait TypedTextDelimited[T] extends SchemedSource with Mappable[T] with TypedSink[T] {
  def typeDescriptor: TypeDescriptor[T]

  protected def separator: String

  /*
   * These options make the string parsing strict. If you want
   * to try to ignore some errors, you can change them, but refer
   * to the cascading documentation on TextDelimited
   */
  protected def strict: Boolean = true
  protected def safe: Boolean = true

  /*
   * Implemented in terms of the above
   */
  override def converter[U >: T] = TupleConverter.asSuperConverter(typeDescriptor.converter)
  override def setter[U <: T] = TupleSetter.asSubSetter(typeDescriptor.setter)
  override def sinkFields = typeDescriptor.fields
  override def sourceFields = typeDescriptor.fields

  override def localScheme =
    new CLTextDelimited(typeDescriptor.fields, false, false, separator, strict, null /* quote */ ,
      typeDescriptor.fields.getTypesClasses, safe)

  override def hdfsScheme =
    HadoopSchemeInstance(new CHTextDelimited(typeDescriptor.fields, null /* compression */ , false, false,
      separator, strict, null /* quote */ ,
      typeDescriptor.fields.getTypesClasses, safe).asInstanceOf[Scheme[_, _, _, _, _]])
}

trait LzoTypedTextDelimited[T] extends TypedTextDelimited[T] with LocalTapSource {
  override def hdfsScheme =
    HadoopSchemeInstance(new LzoTextDelimited(typeDescriptor.fields, false, false,
      separator, strict, null /* quote */ ,
      typeDescriptor.fields.getTypesClasses, safe).asInstanceOf[Scheme[_, _, _, _, _]])
}

class TimePathTypedText[T](path: String, sep: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends TimePathedSource(path, dr, DateOps.UTC) with TypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class TimePathLzoTypedText[T](path: String, sep: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends TimePathedSource(path, dr, DateOps.UTC) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class MostRecentTypedText[T](path: String, sep: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends MostRecentGoodSource(path, dr, DateOps.UTC) with TypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class MostRecentLzoTypedText[T](path: String, sep: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends MostRecentGoodSource(path, dr, DateOps.UTC) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class FixedTypedText[T](path: String, sep: String)(implicit td: TypeDescriptor[T])
  extends FixedPathSource(path) with TypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class FixedLzoTypedText[T](path: String, sep: String)(implicit td: TypeDescriptor[T])
  extends FixedPathSource(path) with LzoTypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}
