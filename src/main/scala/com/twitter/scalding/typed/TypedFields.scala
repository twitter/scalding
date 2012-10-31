package com.twitter.scalding.typed

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding._

trait TypedFields extends Job {
  override implicit def p2rp(pipe : Pipe) : TypedFieldsRichPipe = new TypedFieldsRichPipe(pipe)
  override implicit def source2rp(src : Source) : TypedFieldsRichPipe = new TypedFieldsRichPipe(src.read)
  implicit def gb2tfgb(groupBuilder : GroupBuilder) : TypedFieldsGroupBuilder = new TypedFieldsGroupBuilder(groupBuilder.groupFields)
}

class TypedFieldsRichPipe(pipe: Pipe) extends RichPipe(pipe) with GeneratedRichPipeOperations

class TypedFieldsGroupBuilder(fields: Fields) extends GroupBuilder(fields)
  with GeneratedFoldOperations[GroupBuilder]
  with GeneratedStreamOperations[GroupBuilder]