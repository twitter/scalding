package com.twitter.scalding.parquet.tuple.macros.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import parquet.io.api.RecordConsumer
import parquet.schema.MessageType

import scala.reflect.macros.Context

object WriteSupportProvider {

  def toWriteSupportImpl[T](ctx: Context)(implicit T: ctx.WeakTypeTag[T]): ctx.Expr[(T, RecordConsumer, MessageType) => Unit] = {
    import ctx.universe._

    if (!IsCaseClassImpl.isCaseClassType(ctx)(T.tpe))
      ctx.abort(ctx.enclosingPosition,
        s"""We cannot enforce ${T.tpe} is a case class,
            either it is not a case class or this macro call is possibly enclosed in a class.
            This will mean the macro is operating on a non-resolved type.""")

    def matchField(idx: Int, fieldType: Type, fValue: Tree, groupName: TermName): (Int, Tree) = {
      def writePrimitiveField(wTree: Tree) =
        (idx + 1, q"""rc.startField($groupName.getFieldName($idx), $idx)
                      $wTree
                      rc.endField($groupName.getFieldName($idx), $idx)""")

      def writeGroupField(subTree: Tree) =
        q"""rc.startField($groupName.getFieldName($idx), $idx)
            rc.startGroup()
            $subTree
            rc.endGroup()
            rc.endField($groupName.getFieldName($idx), $idx)
         """
      fieldType match {
        case tpe if tpe =:= typeOf[String] =>
          writePrimitiveField(q"rc.addBinary(Binary.fromString($fValue))")
        case tpe if tpe =:= typeOf[Boolean] =>
          writePrimitiveField(q"rc.addBoolean($fValue)")
        case tpe if tpe =:= typeOf[Short] =>
          writePrimitiveField(q"rc.addInteger($fValue.toInt)")
        case tpe if tpe =:= typeOf[Int] =>
          writePrimitiveField(q"rc.addInteger($fValue)")
        case tpe if tpe =:= typeOf[Long] =>
          writePrimitiveField(q"rc.addLong($fValue)")
        case tpe if tpe =:= typeOf[Float] =>
          writePrimitiveField(q"rc.addFloat($fValue)")
        case tpe if tpe =:= typeOf[Double] =>
          writePrimitiveField(q"rc.addDouble($fValue)")
        case tpe if tpe =:= typeOf[Byte] =>
          writePrimitiveField(q"rc.addInteger($fValue.toInt)")
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val cacheName = newTermName(ctx.fresh(s"optionIndex"))
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head
          val (_, subTree) = matchField(idx, innerType, q"$cacheName", groupName)
          (idx + 1, q"""
                       if($fValue.isDefined) {
                         val $cacheName = $fValue.get
                         $subTree
                       }
                     """)

        case tpe if IsCaseClassImpl.isCaseClassType(ctx)(tpe) =>
          val newGroupName = createGroupName()
          val (_, subTree) = expandMethod(tpe, fValue, newGroupName)
          (idx + 1,
            q"""
               val $newGroupName = $groupName.getType($idx).asGroupType()
               ${writeGroupField(subTree)}""")

        case _ => ctx.abort(ctx.enclosingPosition, s"Case class $T is not pure primitives or nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, pValueTree: Tree, groupName: TermName): (Int, Tree) = {
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .foldLeft((0, q"")) {
          case ((idx, existingTree), getter) =>
            val (newIdx, subTree) = matchField(idx, getter.returnType, q"$pValueTree.$getter", groupName)
            (newIdx, q"""
                      $existingTree
                      $subTree
                    """)
        }
    }

    def createGroupName(): TermName = newTermName(ctx.fresh("group"))

    val rootGroupName = createGroupName()

    val (finalIdx, funcBody) = expandMethod(T.tpe, q"t", rootGroupName)

    if (finalIdx == 0)
      ctx.abort(ctx.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")

    val writeFunction: Tree = q"""
      val writeFunc = (t: $T, rc: _root_.parquet.io.api.RecordConsumer, schema: _root_.parquet.schema.MessageType) => {

        var $rootGroupName: _root_.parquet.schema.GroupType = schema
        rc.startMessage
        $funcBody
        rc.endMessage
      }
      writeFunc
      """
    ctx.Expr[(T, RecordConsumer, MessageType) => Unit](q"$writeFunction")
  }
}
