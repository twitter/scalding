package com.twitter.scalding.source

import cascading.scheme.Scheme
import cascading.scheme.hadoop.{ TextDelimited => CHTextDelimited }
import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink

/**
 * This object gives you easy access to text formats (possibly LZO compressed) by
 * using a case class to describe the field names and types.
 */
case class TypedSep(str: String) extends AnyVal

object TypedText {

  val TAB = TypedSep("\t")
  val ONE = TypedSep("\u0001")
  val COMMA = TypedSep(",")

  def tsv[T: TypeDescriptor](path: String*): Source with TypedTextDelimited[T] =
    new FixedTypedText[T](TAB, path: _*)
  def osv[T: TypeDescriptor](path: String*): Source with TypedTextDelimited[T] =
    new FixedTypedText[T](ONE, path: _*)
  def csv[T: TypeDescriptor](path: String*): Source with TypedTextDelimited[T] =
    new FixedTypedText[T](COMMA, path: _*)

  /**
   * Prefix might be "/logs/awesome"
   */
  private def hourly[T](sep: TypedSep, prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](sep, prefix + TimePathedSource.YEAR_MONTH_DAY_HOUR + "/*")
  }

  def hourlyTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    hourly(TAB, prefix)

  def hourlyOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    hourly(ONE, prefix)

  def hourlyCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    hourly(COMMA, prefix)

  private def daily[T](
      sep: TypedSep, prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    new TimePathTypedText[T](sep, prefix + TimePathedSource.YEAR_MONTH_DAY + "/*")
  }

  def dailyTsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    daily(TAB, prefix)

  def dailyOsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    daily(ONE, prefix)

  def dailyCsv[T](prefix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    daily(COMMA, prefix)

  private def dailyPrefixSuffix[T](
      sep: TypedSep, 
      prefix: String, 
      suffix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = {
    require(prefix.last != '/', "prefix should not include trailing /")
    require(suffix.head == '/', "suffix should include a preceding /")
    new TimePathTypedText[T](sep, prefix + TimePathedSource.YEAR_MONTH_DAY + suffix + "/*")
  }

  def dailyPrefixSuffixTsv[T](prefix: String, suffix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    dailyPrefixSuffix(TAB, prefix, suffix)

  def dailyPrefixSuffixOsv[T](prefix: String, suffix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    dailyPrefixSuffix(ONE, prefix, suffix)

  def dailyPrefixSuffixCsv[T](prefix: String, suffix: String)(implicit dr: DateRange, td: TypeDescriptor[T]): TypedTextDelimited[T] = 
    dailyPrefixSuffix(COMMA, prefix, suffix)

}

trait TypedTextDelimited[T] extends SchemedSource with Mappable[T] with TypedSink[T] {
  def typeDescriptor: TypeDescriptor[T]

  protected def separator: TypedSep

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
    new CLTextDelimited(typeDescriptor.fields, false, false, separator.str, strict, null /* quote */ ,
      typeDescriptor.fields.getTypesClasses, safe)

  override def hdfsScheme =
    HadoopSchemeInstance(new CHTextDelimited(typeDescriptor.fields, null /* compression */ , false, false,
      separator.str, strict, null /* quote */ ,
      typeDescriptor.fields.getTypesClasses, safe).asInstanceOf[Scheme[_, _, _, _, _]])
}

class TimePathTypedText[T](sep: TypedSep, path: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends TimePathedSource(path, dr, DateOps.UTC) with TypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class MostRecentTypedText[T](sep: TypedSep, path: String)(implicit dr: DateRange, td: TypeDescriptor[T])
  extends MostRecentGoodSource(path, dr, DateOps.UTC) with TypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

class FixedTypedText[T](sep: TypedSep, path: String*)(implicit td: TypeDescriptor[T])
  extends FixedPathSource(path: _*) with TypedTextDelimited[T] {
  override def typeDescriptor = td
  protected override def separator = sep
}

