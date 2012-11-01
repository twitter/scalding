package com.twitter.scalding.typed

import cascading.pipe.{Every, Pipe}
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.Dsl.fieldFields
import cascading.operation.Buffer

trait TypedFields extends Job {
  override implicit def p2rp(pipe : Pipe) : TypedFieldsRichPipe = new TypedFieldsRichPipe(pipe)
  override implicit def source2rp(src : Source) : TypedFieldsRichPipe = new TypedFieldsRichPipe(src.read)
}

class TypedFieldsRichPipe(pipe: Pipe) extends RichPipe(pipe) with GeneratedRichPipeOperations {

  def groupBy(f : Field[_]*)(builder : TypedFieldsGroupBuilder => GroupBuilder) : Pipe = {
    builder(new TypedFieldsGroupBuilder(f)).schedule(pipe.getName, pipe)
  }



}

class TypedFieldsGroupBuilder(groupFields: Seq[Field[_]]) extends GroupBuilder(groupFields)
  with FoldOperations[TypedFieldsGroupBuilder]
  with StreamOperations[TypedFieldsGroupBuilder]
  with GeneratedFoldOperations[TypedFieldsGroupBuilder]
  with GeneratedStreamOperations[TypedFieldsGroupBuilder] {

  override def spillThreshold(t : Int) : TypedFieldsGroupBuilder = {

    super.spillThreshold(t).asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def buffer(args : Fields)(b : Buffer[_]) : TypedFieldsGroupBuilder = {

    super.buffer(args)(b).asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def every(ev : Pipe => Every) : GroupBuilder = {

    super.every(ev).asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def foldLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
    (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : TypedFieldsGroupBuilder = {

    super.foldLeft(fieldDef)(init)(fn)(setter, conv)
      .asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def mapReduceMap[T,X,U](fieldDef : (Fields, Fields))(mapfn : T => X )(redfn : (X, X) => X)
                         (mapfn2 : X => U)(implicit startConv : TupleConverter[T],
                                           middleSetter : TupleSetter[X],
                                           middleConv : TupleConverter[X],
                                           endSetter : TupleSetter[U]) : TypedFieldsGroupBuilder = {

    super.mapReduceMap(fieldDef)(mapfn)(redfn)(mapfn2)(startConv, middleSetter, middleConv, endSetter)
      .asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def reverse : TypedFieldsGroupBuilder = {

    super.reverse.asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def mapStream[T,X](fieldDef : (Fields,Fields))(mapfn : (Iterator[T]) => TraversableOnce[X])
                    (implicit conv : TupleConverter[T], setter : TupleSetter[X]) : TypedFieldsGroupBuilder = {

    super.mapStream(fieldDef)(mapfn)(conv, setter)
      .asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def scanLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
               (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : TypedFieldsGroupBuilder = {

    super.scanLeft(fieldDef)(init)(fn)(setter, conv)
      .asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def sortBy(f : Fields) : TypedFieldsGroupBuilder = {

    super.sortBy(f).asInstanceOf[TypedFieldsGroupBuilder]

  }

  override def then(fn : GroupBuilder => GroupBuilder) : TypedFieldsGroupBuilder = {

    super.then(fn).asInstanceOf[TypedFieldsGroupBuilder]

  }

}