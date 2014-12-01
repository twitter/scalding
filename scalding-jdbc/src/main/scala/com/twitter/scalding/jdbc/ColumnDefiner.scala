package com.twitter.scalding.jdbc

case class ColumnName(get: String)
case class Definition(get: String)

case class ColumnDefinition(name: ColumnName, definition: Definition)

trait ColumnDefiner {
  def columns: Iterable[ColumnDefinition]

  protected def columnNames: Array[ColumnName] = columns.map(_.name).toArray
  protected def columnDefinitions: Array[Definition] = columns.map(_.definition).toArray

  // Some helper methods that we can use to generate column definitions
  protected def bigint(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[BIGINT.type]) = coldef(name, nullable)
  protected def int(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[INT.type]) = coldef(name, nullable)
  protected def smallint(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[SMALLINT.type]) = coldef(name, nullable)
  protected def tinyint(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[TINYINT.type]) = coldef(name, nullable)
  protected def varchar(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[VARCHAR.type]) = coldef(name, nullable)
  protected def date(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[DATE.type]) = coldef(name, nullable)
  protected def datetime(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[DATETIME.type]) = coldef(name, nullable)
  protected def text(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[TEXT.type]) = coldef(name, nullable)
  protected def double(name: String, nullable: IsNullable = NotNullable)(implicit coldef: DriverColumnDefiner[DOUBLE.type]) = coldef(name, nullable)
}
