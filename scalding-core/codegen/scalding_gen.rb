#!/usr/bin/env ruby

$indent = "  "

def make_tuple_conv(cnt)
  type_names = ('A'..'Z').to_a[0...cnt]
  indices = (0...cnt).to_a
  comma_tn = type_names.join(",")
  getters = type_names.map { |n|
              #"  g#{n} : TupleGetter[#{n}]"
              "  g#{n} : TupleGetter[#{n}]"
            }.join(",\n#{$indent}")
  typed_args = type_names.zip(indices).map { |n,ni|
                 "g#{n}.get(tup, #{ni})"
               }.join(",\n#{$indent}       ")
  %Q|\n#{$indent}implicit def tuple#{cnt}Converter[#{comma_tn}](implicit
#{$indent}#{getters}): TupleConverter[Tuple#{cnt}[#{comma_tn}]] = new TupleConverter[Tuple#{cnt}[#{comma_tn}]]{
#{$indent}    def apply(te : TupleEntry) = {
#{$indent}      val tup = te.getTuple
#{$indent}      Tuple#{cnt}(#{typed_args})
#{$indent}    }
#{$indent}    def arity = #{cnt}
#{$indent}}
|
end

def make_setter(cnt)
  underscores = (["_"]*cnt).join(",")
  type_names = ('A'..'Y').to_a[0...cnt]
  comma_tn = type_names.join(",")
  head = %Q|\n#{$indent}implicit def tup#{cnt}Setter[Z <: Tuple#{cnt}[#{underscores}]]: TupleSetter[Z] = new TupleSetter[Z] {
#{$indent}  override def apply(arg: Z) = {
#{$indent}    val tup = Tuple.size(#{cnt})
#{$indent}    |
  middle = (1..cnt).map {|c| "tup.set(#{c-1}, arg._#{c})" }.join("\n#{$indent}    ")
  tail = %Q|
#{$indent}    tup
#{$indent}  }

#{$indent}  override def arity = #{cnt}
#{$indent}}|
  head + middle + tail
end

def make_typer(cnt)
  type_names = type_names = ('A'..'Y').to_a[0...cnt]
  comma_tn = type_names.join(",")
  manifests = type_names.map { |n| "#{n.downcase}: Manifest[#{n}]" }.join(",")
  classes = type_names.map { |n| "#{n.downcase}.erasure" }.join(",")
%Q|
#{$indent}implicit def tuple#{cnt}Typer[#{comma_tn}](implicit #{manifests}): FieldsTyper[Tuple#{cnt}[#{comma_tn}]]
#{$indent}  = new FieldsTyper[Tuple#{cnt}[#{comma_tn}]] {
#{$indent}    override def getListOfClasses = Array[Type](#{classes})
#{$indent}}
|
end

puts "// following were autogenerated by #{__FILE__} at #{Time.now} do not edit"
puts %q|package com.twitter.scalding

import java.lang.reflect.Type

import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

trait GeneratedTupleConverters extends LowPriorityTupleConverters {
|

(1..22).each { |c| puts make_tuple_conv(c) }
puts "}"
puts "trait GeneratedTupleSetters extends LowPriorityTupleSetters {"
(1..22).each { |c| puts make_setter(c) }

puts "}"
puts "trait GeneratedFieldsTypers extends LowPriorityFieldsTypers {"
(1..22).each { |c| puts make_typer(c) }

puts "}"
puts "// end of autogenerated"
