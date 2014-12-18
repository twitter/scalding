package com.twitter.scalding

object TypedArgs {
  implicit final def typedArgs(args: Args): TypedArgs = new TypedArgs(args)
}

class TypedArgs(val args: Args) extends AnyVal {

  private def asType[T](f: (String, String) => T): (String, String) => T = {
    (key: String, value: String) =>
      {
        try {
          f(key, value)
        } catch {
          case e: NumberFormatException =>
            throw ArgsException("Invalid value %s for --%s".format(value, key))
        }
      }
  }

  private def asInt = asType((name, value) => java.lang.Integer.parseInt(value))
  private def asDouble = asType((name, value) => java.lang.Double.parseDouble(value))
  private def asLong = asType((name, value) => java.lang.Long.parseLong(value))
  private def asFloat = asType((name, value) => java.lang.Float.parseFloat(value))

  def int(key: String, default: Int): Int = {
    args.optional(key).map(asInt(key, _)).getOrElse(default)
  }

  def int(key: String): Int = {
    asInt(key, args(key))
  }

  def long(key: String, default: Long): Long = {
    args.optional(key).map(asLong(key, _)).getOrElse(default)
  }

  def long(key: String): Long = {
    asLong(key, args(key))
  }

  def float(key: String, default: Float): Float = {
    args.optional(key).map(asFloat(key, _)).getOrElse(default)
  }

  def float(key: String): Float = {
    asFloat(key, args(key))
  }

  def double(key: String, default: Double): Double = {
    args.optional(key).map(asDouble(key, _)).getOrElse(default)
  }

  def double(key: String): Double = {
    asDouble(key, args(key))
  }

}
