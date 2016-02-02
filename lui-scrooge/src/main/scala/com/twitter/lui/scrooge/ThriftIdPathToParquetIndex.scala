package com.twitter.lui.scrooge

import org.apache.parquet.schema.{ Type => ParquetSchemaType, MessageType, GroupType }
import org.apache.parquet.thrift.struct.ThriftType._
import org.apache.parquet.thrift.struct.ThriftType
import org.apache.parquet.thrift.struct.ThriftField
import com.twitter.algebird.{ Monoid, Semigroup, MapAlgebra }
import scala.collection.JavaConverters._

case class PathToSchemaType(thriftIdPath: Vector[Short],
  parquetIdPath: Vector[Int],
  parquetStringPath: Vector[String],
  parquetSchema: ParquetSchemaType,
  insideCollection: Boolean)

// A path of indices within groups to a field
case class ThriftIdPath(toVector: Vector[Short]) extends AnyVal

case class ParquetFieldInfo(path: Vector[Int],
  strPath: Vector[String],
  parquetSchema: ParquetSchemaType,
  closestPrimitiveChild: Option[ThriftIdPath],
  maxDefinitionLevel: Int = 0, // These unfortunately aren't set in this file yet. Come in later
  maxRepetitionLevel: Int = 0,
  thriftType: ThriftType,
  insideCollection: Boolean) {
  def isPrimitive: Boolean = parquetSchema.isPrimitive
}

case class ParquetMappingToThriftMapping(m: List[(ThriftIdPath, ParquetFieldInfo)]) extends AnyVal

object ParquetMappingToThriftMappingMonoid extends Monoid[ParquetMappingToThriftMapping] {
  def zero = ParquetMappingToThriftMapping(List())

  private[this] def exceptionSemigroup[T] = new Semigroup[T] {
    def plus(a: T, b: T) = sys.error(s"Fields $a and $b should not be combined. ExceptionSemigroup!.")
  }

  def plus(a: ParquetMappingToThriftMapping, b: ParquetMappingToThriftMapping): ParquetMappingToThriftMapping = {
    implicit val thriftIdSg = exceptionSemigroup[ParquetFieldInfo]
    val combined = Monoid.plus(a.m, b.m)
    require(combined.size == a.m.size + b.m.size)
    ParquetMappingToThriftMapping(combined)
  }
}

/**
 * Creates a mapping from thrift id path -> the index of the field in the parquet schema that was used
 * to write the file. This is important, it allows us to reconcile the fact that parquet schema fields are keyed by name,
 * but thrift fields are keyed by ID. So here we treat the id path as the source of truth, then figure out what that id path
 * represented in the schema used to write the file. This lets us support renamed fields, as well as fields that are out of order
 * in the IDL file, as well as fields that were inserted later out of order in the idl file.
 */
object ThriftIdPathToParquetIndex extends StateVisitor[ParquetMappingToThriftMapping, PathToSchemaType] {
  implicit val pmtmMonoid = ParquetMappingToThriftMappingMonoid
  def get(fileSchema: MessageType, fileDescriptor: StructType) = {
    val recursive = fileDescriptor.accept(this, PathToSchemaType(Vector(), Vector(), Vector(), fileSchema, false))

    val primitiveChild = Some(recursive.m.iterator.filter(_._2.isPrimitive).next._1)

    Monoid.plus(
      ParquetMappingToThriftMapping(
        List(
          (ThriftIdPath(Vector()), ParquetFieldInfo(Vector(), Vector(), fileSchema, primitiveChild, 0, 0, fileDescriptor, false)))),
      recursive)
  }

  override def visit(structType: StructType, state: PathToSchemaType): ParquetMappingToThriftMapping = {
    val groupType: GroupType = state.parquetSchema.asGroupType()
    // Loop through the thrift fields coming from the side metadata
    Monoid.sum(structType.getChildren.iterator.asScala.flatMap { c: ThriftField =>

      val childIndex: Int = groupType.getFieldIndex(c.getName)
      val parquetStringPath: Vector[String] = state.parquetStringPath :+ c.getName
      val parquetIdPath: Vector[Int] = state.parquetIdPath :+ childIndex

      val matchingChild: ParquetSchemaType = groupType.getType(childIndex)

      val childPathToSchemaType = PathToSchemaType(state.thriftIdPath :+ c.getFieldId, parquetIdPath, parquetStringPath, matchingChild, state.insideCollection)
      val childsMaps = c.getType.accept(this, childPathToSchemaType)
      val primitiveChild = if (matchingChild.isPrimitive) None else Some(childsMaps.m.iterator.filter(_._2.isPrimitive).next._1)

      val childNodeValue = ParquetMappingToThriftMapping(
        List(ThriftIdPath(childPathToSchemaType.thriftIdPath) -> ParquetFieldInfo(parquetIdPath, parquetStringPath, matchingChild, primitiveChild, 0, 0, c.getType, state.insideCollection)))
      List(childNodeValue, childsMaps)
    })
  }

  override def visit(mapType: MapType, state: PathToSchemaType): ParquetMappingToThriftMapping = {
    val mapGroupType = state.parquetSchema.asGroupType()

    val groupType = mapGroupType.getType(0).asGroupType()

    val keyT = (groupType.getType(0), 1, mapType.getKey.getType)
    val valueT = (groupType.getType(1), 2, mapType.getValue.getType)

    Monoid.sum(List(keyT, valueT).flatMap {
      case (parquetCType, pathExtension, thriftInnerType) =>
        val parquetStringPath: Vector[String] = state.parquetStringPath ++ Vector("map", parquetCType.getName)
        val parquetIdPath: Vector[Int] = state.parquetIdPath :+ pathExtension

        val childPathToSchemaType = PathToSchemaType(state.thriftIdPath :+ pathExtension.toShort, parquetIdPath, parquetStringPath, parquetCType, true)
        val childsMaps = thriftInnerType.accept(this, childPathToSchemaType)
        val primitiveChild = if (parquetCType.isPrimitive) None else Some(childsMaps.m.iterator.filter(_._2.isPrimitive).next._1)

        val childNodeValue = ParquetMappingToThriftMapping(
          List(ThriftIdPath(childPathToSchemaType.thriftIdPath) -> ParquetFieldInfo(parquetIdPath, parquetStringPath, parquetCType, primitiveChild, 0, 0, thriftInnerType, true)))
        List(childNodeValue, childsMaps)
    })

  }

  override def visit(setType: SetType, state: PathToSchemaType): ParquetMappingToThriftMapping = {
    val groupType: GroupType = state.parquetSchema.asGroupType()
    // Loop through the thrift fields coming from the side metadata
    val c: ThriftType = setType.getValues.getType
    val parquetCType = groupType.getType(0)
    Monoid.sum(List(c).flatMap { c: ThriftType =>

      val parquetStringPath: Vector[String] = state.parquetStringPath :+ parquetCType.getName
      val parquetIdPath: Vector[Int] = state.parquetIdPath

      val childPathToSchemaType = PathToSchemaType(state.thriftIdPath :+ 1.toShort, parquetIdPath, parquetStringPath, parquetCType, true)
      val childsMaps = c.accept(this, childPathToSchemaType)
      val primitiveChild = if (parquetCType.isPrimitive) None else Some(childsMaps.m.iterator.filter(_._2.isPrimitive).next._1)

      val childNodeValue = ParquetMappingToThriftMapping(
        List(ThriftIdPath(childPathToSchemaType.thriftIdPath) -> ParquetFieldInfo(parquetIdPath, parquetStringPath, parquetCType, primitiveChild, 0, 0, c, true)))
      List(childNodeValue, childsMaps)
    })
  }

  override def visit(listType: ListType, state: PathToSchemaType): ParquetMappingToThriftMapping = {

    val groupType: GroupType = state.parquetSchema.asGroupType()
    // Loop through the thrift fields coming from the side metadata
    val c: ThriftType = listType.getValues.getType
    val parquetCType = groupType.getType(0)
    Monoid.sum(List(c).flatMap { c: ThriftType =>

      val parquetStringPath: Vector[String] = state.parquetStringPath :+ parquetCType.getName
      val parquetIdPath: Vector[Int] = state.parquetIdPath

      val childPathToSchemaType = PathToSchemaType(state.thriftIdPath :+ 1.toShort, parquetIdPath, parquetStringPath, parquetCType, true)
      val childsMaps = c.accept(this, childPathToSchemaType)
      val primitiveChild = if (parquetCType.isPrimitive) None else Some(childsMaps.m.iterator.filter(_._2.isPrimitive).next._1)

      val childNodeValue = ParquetMappingToThriftMapping(
        List(ThriftIdPath(childPathToSchemaType.thriftIdPath) -> ParquetFieldInfo(parquetIdPath, parquetStringPath, parquetCType, primitiveChild, 0, 0, c, true)))
      List(childNodeValue, childsMaps)
    })
    // val groupType = state.parquetSchema.asGroupType()

    // val childPathToSchemaType = PathToSchemaType(state.path :+ 1.toShort, child)
    // val childsMaps = listType.getValues.getType.accept(this, childPathToSchemaType)
    // childsMaps
  }

  override def visit(enumType: EnumType, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

  override def visit(boolType: BoolType, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

  override def visit(byteType: ByteType, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

  override def visit(doubleType: DoubleType, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

  override def visit(i16Type: I16Type, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

  override def visit(i32Type: I32Type, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

  override def visit(i64Type: I64Type, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

  override def visit(stringType: StringType, state: PathToSchemaType): ParquetMappingToThriftMapping = ParquetMappingToThriftMappingMonoid.zero

}
