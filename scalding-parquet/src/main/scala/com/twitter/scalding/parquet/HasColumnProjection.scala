package com.twitter.scalding.parquet

trait HasColumnProjection[This <: HasColumnProjection[This]] {

  final def withColumns(columnGlobs: String*): This = {
    val globs = columnGlobs.map(ColumnProjectionGlob(_))
    copyWithColumnGlobs(this.columnGlobs ++ globs)
  }

  protected[parquet] def columnGlobs: Set[ColumnProjectionGlob] = Set()

  /**
   * Parquet accepts globs separated by the ; character
   */
  final protected[parquet] def globsInParquetStringFormat: Option[String] =
    if (columnGlobs.isEmpty) None else Some(columnGlobs.iterator.map(_.glob).mkString(";"))

  /**
   * Subclasses must implement this method to return a copy of themselves,
   * but must override columnGlobs to return globs.
   */
  protected def copyWithColumnGlobs(columnGlobs: Set[ColumnProjectionGlob]): This
}

case class ColumnProjectionGlob(glob: String) {
  require(!glob.contains(";"), "A column projection glob cannot contain a ; character")
}