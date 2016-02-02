package com.twitter.lui.scrooge

import org.apache.parquet.thrift.struct.ThriftType._

import scala.collection.JavaConverters._

/**
 * Creates a mapping from a path of thrift field IDs -> metadata about the enum at this position in the schema
 *
 * This is important because we need to know what the enums looked like at the time the file was written, so we can
 * map them back into today's read schema (so that we allow renaming enums so long as their backing int doesn't change)
 */
case class ParquetThriftEnumInfo(m: Map[String, Int]) extends AnyVal

object ThriftEnumMetadata extends StateVisitor[Map[ThriftIdPath, Map[String, Int]], Vector[Short]] {

  def get(fileDescriptor: StructType) = fileDescriptor.accept(this, Vector())

  override def visit(enumType: EnumType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = {
    val m = enumType.getValues.iterator().asScala.map { e =>
      (e.getName, e.getId)
    }.toMap
    Map(ThriftIdPath(state) -> m)
  }

  override def visit(mapType: MapType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = {
    val k = mapType.getKey.getType.accept(this, state :+ 1.toShort)
    val v = mapType.getValue.getType.accept(this, state :+ 2.toShort)
    k ++ v
  }

  override def visit(setType: SetType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] =
    setType.getValues.getType.accept(this, state :+ 1.toShort)

  override def visit(listType: ListType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] =
    listType.getValues.getType.accept(this, state :+ 1.toShort)

  override def visit(structType: StructType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = {
    val children = structType.getChildren.asScala.map { c =>
      c.getType.accept(this, state :+ c.getFieldId)
    }
    children.reduce[Map[ThriftIdPath, Map[String, Int]]]{ case (m1, m2) => m1 ++ m2 }
  }

  override def visit(boolType: BoolType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = Map()
  override def visit(byteType: ByteType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = Map()
  override def visit(doubleType: DoubleType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = Map()
  override def visit(i16Type: I16Type, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = Map()
  override def visit(i32Type: I32Type, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = Map()
  override def visit(i64Type: I64Type, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = Map()
  override def visit(stringType: StringType, state: Vector[Short]): Map[ThriftIdPath, Map[String, Int]] = Map()
}
