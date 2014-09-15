package com.twitter.scalding.parquet.avro

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Helps create projections for Avro schemas.
 */
object Projection {
  def apply[T <: SpecificRecord : ClassTag](fields: String*) : Schema = apply(Avro.getSchema[T], fields.toSet)

  def apply[T <: SpecificRecord : ClassTag](fields: Set[String]) : Schema = apply(Avro.getSchema[T], fields)

  /**
   * Create a projection for {{schema}} that includes {{fields}}.
   */
  def apply(schema: Schema, fields: Set[String]) : Schema = {
    createProjection(schema, fields)
  }

  private def createProjection(schema: Schema, fields: Set[String], parentFieldName: Option[String] = None): Schema = {
    schema.getType match {
      case Schema.Type.RECORD => createRecordProjection(schema, fields, parentFieldName)
      case Schema.Type.UNION  => createUnionProjection(schema, fields, parentFieldName)
      case Schema.Type.ARRAY  => createArrayProjection(schema, fields, parentFieldName)

      case _ =>
        val fieldInfo = parentFieldName.map(_ + ":").getOrElse("") + schema.getType
        val children = fields.mkString(", ")
        throw new RuntimeException(s"Projection doesn't support schema type $fieldInfo with fields: $children")
    }
  }

  private def createRecordProjection(schema: Schema, fields: Set[String], parentFieldName: Option[String]): Schema = {
    // Take the head of any nested properties, "parent.fieldX" => "parent"
    val nestedFields = fields.filter(_.contains('.')).map(_.split('.').head)
    val directFields = fields ++ nestedFields

    val schemaFields = schema.getFields.asScala
    validateRequestedFields(schema, schemaFields, directFields)

    val pFields = schemaFields.filter(f => directFields.contains(f.name())).map { f =>

      // Create projection for the nested field
      val schema =
        if (nestedFields.contains(f.name())) {
          val prefix = f.name() + "."
          // Find the nested fields and remove the prefix
          val children = fields.filter(_.startsWith(prefix)).map(_.substring(prefix.length))
          createProjection(f.schema(), children, fullFieldName(parentFieldName, f.name()))
        }
        else f.schema()

      copyField(schema, f)
    }

    val projection = Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, false)
    projection.setFields(pFields.asJava)
    projection
  }

  private def createUnionProjection(schema: Schema, fields: Set[String], parentFieldName: Option[String]): Schema = {
    val projectedSchemas = schema.getTypes.asScala.map { nestedSchema =>
      nestedSchema.getType match {
        case Schema.Type.NULL => nestedSchema

        case _ => createProjection(nestedSchema, fields, parentFieldName)
      }
    }

    Schema.createUnion(projectedSchemas.asJava)
  }

  private def createArrayProjection(schema: Schema, fields: Set[String], parentFieldName: Option[String]): Schema = {
    Schema.createArray(
      createProjection(schema.getElementType, fields, parentFieldName)
    )
  }

  private def validateRequestedFields(schema: Schema, schemaFields: Seq[Schema.Field], fieldNames: Set[String]) {
    fieldNames.filter(_.contains('.') == false).foreach { field =>
      if (!schemaFields.exists(_.name() == field))
        throw new RuntimeException(
          s"Field $field not found in schema ${schema.getFullName}. " +
          s"Supported fields are: ${schemaFields.map(_.name).mkString(", ")}"
        )
    }
  }

  private def copyField(schema: Schema, field: Schema.Field): Schema.Field =
    new CustomField(schema, field).asInstanceOf[Schema.Field]

  //remember the original field position
  private class CustomField(schema: Schema, field: Schema.Field)
    extends Schema.Field(field.name(), schema, field.doc(), field.defaultValue()) {

    val originalPos = field.pos()

    override def pos(): Int = originalPos
  }

  private def fullFieldName(parentFieldName: Option[String], fieldName: String): Some[String] =
    parentFieldName match {
      case Some(parent) => Some(parent + "." + fieldName)
      case _            => Some(fieldName)
    }
}
