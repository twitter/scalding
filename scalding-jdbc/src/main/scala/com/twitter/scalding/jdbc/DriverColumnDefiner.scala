package com.twitter.scalding.jdbc

sealed trait JdbcType
case object BIGINT extends JdbcType
case object INT extends JdbcType
case object SMALLINT extends JdbcType
case object TINYINT extends JdbcType
case object BOOLEAN extends JdbcType
case object VARCHAR extends JdbcType
case object DATE extends JdbcType
case object DATETIME extends JdbcType
case object TEXT extends JdbcType
case object DOUBLE extends JdbcType

object IsNullable {
  def apply(isNullable: Boolean): IsNullable = if (isNullable) Nullable else NotNullable
}
sealed abstract class IsNullable(val get: String)
case object Nullable extends IsNullable("NULL")
case object NotNullable extends IsNullable("NOT NULL")

/**
 * This is a mechanism by which different databases can control and configure the way in which statements are created.
 */
trait DriverColumnDefiner[Type <: JdbcType] {
  //TODO does this need to deal with sizes, or now that it's fixed per DB will that be fine?
  def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition

  //TODO should use the fact that now we have more typed typeName
  protected def mkColumnDef(
    name: String,
    typeName: String,
    nullable: IsNullable,
    sizeOp: Option[Int] = None,
    defOp: Option[String]) = {
    val sizeStr = sizeOp.map { "(" + _.toString + ")" }.getOrElse("")
    val defStr = defOp.map { " DEFAULT '" + _ + "' " }.getOrElse(" ")
    ColumnDefinition(ColumnName(name), Definition(typeName + sizeStr + defStr + nullable.get))
  }

}

trait MysqlTableCreationImplicits {
  implicit val bigint: DriverColumnDefiner[BIGINT.type] =
    new DriverColumnDefiner[BIGINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "BIGINT", nullable, Some(20), None)
    }

  implicit val int: DriverColumnDefiner[INT.type] =
    new DriverColumnDefiner[INT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "INT", nullable, Some(11), None)
    }

  implicit val smallint: DriverColumnDefiner[SMALLINT.type] =
    new DriverColumnDefiner[SMALLINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "SMALLINT", nullable, Some(6), Some("0"))
    }

  // NOTE: tinyint(1) actually gets converted to a java Boolean
  implicit val tinyint: DriverColumnDefiner[TINYINT.type] =
    new DriverColumnDefiner[TINYINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "TINYINT", nullable, Some(6), None)
    }

  implicit val varchar: DriverColumnDefiner[VARCHAR.type] =
    new DriverColumnDefiner[VARCHAR.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "VARCHAR", nullable, Some(255), None)
    }

  implicit val date: DriverColumnDefiner[DATE.type] =
    new DriverColumnDefiner[DATE.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DATE", nullable, None, None)
    }

  implicit val datetime: DriverColumnDefiner[DATETIME.type] =
    new DriverColumnDefiner[DATETIME.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DATETIME", nullable, None, None)
    }

  implicit val text: DriverColumnDefiner[TEXT.type] =
    new DriverColumnDefiner[TEXT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "TEXT", nullable, None, None)
    }

  implicit val double: DriverColumnDefiner[DOUBLE.type] =
    new DriverColumnDefiner[DOUBLE.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DOUBLE", nullable, None, None)
    }
}

trait VerticaTableCreationImplicits {
  implicit val bigint: DriverColumnDefiner[BIGINT.type] =
    new DriverColumnDefiner[BIGINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "BIGINT", nullable, None, None)
    }

  implicit val int: DriverColumnDefiner[INT.type] =
    new DriverColumnDefiner[INT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "INT", nullable, None, None)
    }

  implicit val smallint: DriverColumnDefiner[SMALLINT.type] =
    new DriverColumnDefiner[SMALLINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "SMALLINT", nullable, None, Some("0"))
    }

  implicit val tinyint: DriverColumnDefiner[TINYINT.type] =
    new DriverColumnDefiner[TINYINT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "TINYINT", nullable, None, None)
    }

  implicit val varchar: DriverColumnDefiner[VARCHAR.type] =
    new DriverColumnDefiner[VARCHAR.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "VARCHAR", nullable, Some(255), None)
    }

  implicit val date: DriverColumnDefiner[DATE.type] =
    new DriverColumnDefiner[DATE.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DATE", nullable, None, None)
    }

  implicit val datetime: DriverColumnDefiner[DATETIME.type] =
    new DriverColumnDefiner[DATETIME.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DATETIME", nullable, None, None)
    }

  implicit val text: DriverColumnDefiner[TEXT.type] =
    new DriverColumnDefiner[TEXT.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "TEXT", nullable, None, None)
    }

  implicit val double: DriverColumnDefiner[DOUBLE.type] =
    new DriverColumnDefiner[DOUBLE.type] {
      override def apply(name: String, nullable: IsNullable = NotNullable): ColumnDefinition =
        mkColumnDef(name, "DOUBLE PRECISION", nullable, None, None)
    }
}
