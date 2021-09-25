package com.twitter.scalding.parquet

import org.slf4j.LoggerFactory

object HasColumnProjection {
  val LOG = LoggerFactory.getLogger(this.getClass)

  def requireNoSemiColon(glob: String) = {
    require(!glob.contains(";"), "A column projection glob cannot contain a ; character")
  }
}

trait HasColumnProjection {
  import com.twitter.scalding.parquet.HasColumnProjection._

  /**
   * Deprecated. Use withColumnProjections, which uses a different glob syntax.
   *
   * The format for specifying columns is described here:
   * https://github.com/apache/parquet-mr/blob/3df3372a1ee7b6ea74af89f53a614895b8078609/parquet_cascading.md#2-projection-pushdown
   * (Note that this link is different from the one below in withColumnProjections)
   *
   * Note that the format described there says that multiple globs can be combined with a ; character.
   * Instead, we use a Set() here and will eventually join the set on the ; character for you.
   */
  @deprecated(message = "Use withColumnProjections, which uses a different glob syntax", since = "0.15.1")
  def withColumns: Set[String] = Set()

  /**
   * The format for specifying columns is described here:
   * https://github.com/apache/parquet-mr/blob/master/parquet_cascading.md#21-projection-pushdown-with-thriftscrooge-records
   *
   * Note that the format described there says that multiple globs can be combined with a ; character.
   * Instead, we use a Set() here and will eventually join the set on the ; character for you.
   */
  def withColumnProjections: Set[String] = Set()

  /**
   * Parquet accepts globs separated by the ; character
   */
  protected[parquet] final def columnProjectionString: Option[ColumnProjectionString] = {
    val deprecated = withColumns
    val strict = withColumnProjections

    require(deprecated.isEmpty || strict.isEmpty,
      "Cannot provide both withColumns and withColumnProjections")

    deprecated.foreach(requireNoSemiColon)
    strict.foreach(requireNoSemiColon)

    if (deprecated.nonEmpty) {
      LOG.warn("withColumns is deprecated. Please use withColumnProjections, which uses a different glob syntax")
      Some(DeprecatedColumnProjectionString(deprecated))
    } else if (strict.nonEmpty) {
      Some(StrictColumnProjectionString(strict))
    } else {
      None
    }
  }
}

sealed trait ColumnProjectionString {
  def globStrings: Set[String]
  def asSemicolonString: String = globStrings.mkString(";")
}
final case class DeprecatedColumnProjectionString(globStrings: Set[String]) extends ColumnProjectionString
final case class StrictColumnProjectionString(globStrings: Set[String]) extends ColumnProjectionString
