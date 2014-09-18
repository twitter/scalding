package com.twitter.scalding.parquet

trait HasColumnProjection {

  def withColumns: Set[String] = Set()

  protected[parquet] final def columnGlobs: Set[ColumnProjectionGlob] = withColumns.map(ColumnProjectionGlob)

  /**
   * Parquet accepts globs separated by the ; character
   */
  protected[parquet] final def globsInParquetStringFormat: Option[String] =
    if (columnGlobs.isEmpty) None else Some(columnGlobs.iterator.map(_.glob).mkString(";"))

}

// TODO: extend AnyVal after scala 2.9 support is dropped
case class ColumnProjectionGlob(glob: String) /* extends AnyVal */ {
  require(!glob.contains(";"), "A column projection glob cannot contain a ; character")
}