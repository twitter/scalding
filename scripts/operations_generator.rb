#!/usr/bin/env ruby

$n = 3

def make_maps(name, fn_builder, mid_type, tuple_builder, base_call, return_type)

  puts "  def #{name}[A,#{mid_type}T](fs : (Fields,Fields))#{fn_builder.call('A','T')}"
  puts "    (implicit #{tuple_builder.call('A','T')}) : #{return_type}"
  puts ""

  (1..$n).each { |in_arity|
    (1..$n).each { |out_arity|
      puts make_map(name, in_arity, out_arity, fn_builder, mid_type, tuple_builder, base_call, return_type)
    }
  }

end

def make_map(name, in_arity, out_arity, fn_builder, mid_type, tuple_builder, base_call, return_type)

  in_type_names = ('A'..'H').to_a[0...in_arity]
  out_type_names = ('S'..'Z').to_a[0...out_arity]
  in_type = type_of(in_type_names)
  out_type = type_of(out_type_names)
  in_converter = if in_arity == 1 then 'fieldToFields' else 'productToFields' end
  out_converter = if out_arity == 1 then 'fieldToFields' else 'productToFields' end
  
  "  def #{name}[#{in_type_names.join(',')},#{mid_type}#{out_type_names.join(',')}]" +
  "(fs : (#{fields_of(in_type_names)},#{fields_of(out_type_names)}))" +
  fn_builder.call(in_type, out_type) +
  %Q|
    (implicit #{tuple_builder.call(in_type,out_type)}, inArity : Arity#{in_arity}, outArity : Arity#{out_arity}) : #{return_type} = {

      #{name}(#{in_converter}(fs._1) -> #{out_converter}(fs._2))#{base_call}

  }

|
end

def make_filters(name, return_type)

  puts "  def #{name}[T](fs : Fields)(fn : (T) => Boolean)"
  puts "    (implicit conv : TupleConverter[T]) : #{return_type}"
  puts ""

  (1..$n).each { |arity| puts make_filter(name, arity, return_type) }

end

def make_filter(name, arity, return_type)

  type_names = ('A'..'H').to_a[0...arity]
  type = type_of(type_names)
  converter = if arity == 1 then 'fieldToFields' else 'productToFields' end
  param_list = type_names.zip(Array(1..arity)).map { |t| "f#{t[1]} : Field[#{t[0]}]" }.join(', ')
  arg_tuple = Array(1..arity).map { |k| "f#{k}" }.join(',')
  arg_tuple = "(" + arg_tuple + ")" if arity > 1

  %Q|  def #{name}[#{type_names.join(',')}](#{param_list})(fn : (#{type}) => Boolean)
    (implicit conv : TupleConverter[#{type}], arity : Arity#{arity}) : #{return_type} = {

      #{name}(#{converter}(#{arg_tuple}))(fn)(conv)

  }

|
end

def make_unary_ops(name, fn_builder, tuple_builder, pred_type, is_sorter, out_type, return_type, base_call)

  extra_params = if is_sorter then ", k : Int" else "" end
  evidence_bound = if pred_type == "" then ":TupleConverter" else "" end

  puts "  def #{name}[#{pred_type}T#{evidence_bound}](fs : (Fields,Fields)#{extra_params})#{fn_builder.call('T')}"
  print "    "
  print "(implicit #{tuple_builder.call('T')}) " if pred_type != ""
  puts ": #{return_type}"
  puts ""
  
  (1..$n).each { |arity|
    puts make_unary_op(name, arity, fn_builder, tuple_builder, pred_type, is_sorter, out_type, return_type, base_call)
  }

end

def make_unary_op(name, arity, fn_builder, tuple_builder, pred_type, is_sorter, out_type, return_type, base_call)

  type_names = ('A'..'H').to_a[0...arity]
  type = type_of(type_names)
  converter = if arity == 1 then 'fieldToFields' else 'productToFields' end
  extra_params = if is_sorter then ", k : Int" else "" end
  extra_args = if is_sorter then ", k" else "" end
  
  %Q|  def #{name}[#{pred_type}#{type_names.join(',')}](fs : (#{fields_of(type_names)},Field[#{out_type.call(type)}])#{extra_params})#{fn_builder.call(type)}
    (implicit #{tuple_builder.call(type)}, arity : Arity#{arity}) : #{return_type} = {

      #{name}(#{converter}(fs._1) -> fieldToFields(fs._2)#{extra_args})#{base_call}

  }

|
end

def make_reduce_ops(name, fn_builder, tuple_builder, return_type, base_call)

  puts "  def #{name}[T](fs : (Fields,Fields))#{fn_builder.call('T')}"
  puts "    (implicit #{tuple_builder.call('T')}) : #{return_type}"
  puts ""

  (1..$n).each { |arity|
    puts make_reduce_op(name, arity, fn_builder, tuple_builder, return_type, base_call)
  }

end

def make_reduce_op(name, arity, fn_builder, tuple_builder, return_type, base_call)

  type_names = ('A'..'H').to_a[0...arity]
  type = type_of(type_names)

  converter = if arity == 1 then 'fieldToFields' else 'productToFields' end
  param_list = type_names.zip(Array(1..arity)).map { |t| "f#{t[1]} : Field[#{t[0]}]" }.join(', ')
  arg_tuple = Array(1..arity).map { |k| "f#{k}" }.join(',')
  arg_tuple = "(" + arg_tuple + ")" if arity > 1

  %Q|  def #{name}[#{type_names.join(',')}](#{param_list})#{fn_builder.call(type)}
    (implicit #{tuple_builder.call(type)}, arity : Arity#{arity}) : #{return_type} = {

      #{name}(#{converter}(#{arg_tuple}) -> #{converter}(#{arg_tuple}))#{base_call}

  }
  
  def #{name}[#{type_names.join(',')}](fs : (#{fields_of(type_names)},#{fields_of(type_names)}))#{fn_builder.call(type)}
    (implicit #{tuple_builder.call(type)}, arity : Arity#{arity}) : #{return_type} = {

      #{name}(#{converter}(fs._1) -> #{converter}(fs._2))#{base_call}

  }

|
end

def fields_of(type_names)

  fields = type_names.map { |type| "Field[" + type + "]" }.join(',')
  if type_names.size > 1 then "(" + fields + ")" else fields end

end

def type_of(type_names)

  type = type_names.join(',')
  type = "(" + type + ")" if type_names.size > 1
  type

end

# Lambda expressions

map_fn_builder = lambda { |in_type,out_type| "(fn : (#{in_type}) => #{out_type})" }
map_tuple = lambda { |in_type,out_type| "conv : TupleConverter[#{in_type}], setter : TupleSetter[#{out_type}]" }
map_base_call = "(fn)(conv, setter)"

flat_map_fn_builder = lambda { |in_type,out_type| "(fn : (#{in_type}) => Iterable[#{out_type}])" }

mapred_map_fn_builder = lambda { |in_type,out_type| "(mapfn : (#{in_type}) => R)(redfn : (R,R) => R)(mapfn2 : R => #{out_type})" }
mapred_map_tuple = lambda { |in_type,out_type| "startConv : TupleConverter[#{in_type}], midSetter : TupleSetter[R], midConv : TupleConverter[R], endSetter : TupleSetter[#{out_type}]" }
mapred_map_base_call = "(mapfn)(redfn)(mapfn2)(startConv, midSetter, midConv, endSetter)"

mapplus_map_fn_builder = lambda { |in_type,out_type| "(mapfn : (#{in_type}) => R)(mapfn2 : R => #{out_type})" }
mapplus_map_tuple = lambda { |in_type,out_type| "startConv : TupleConverter[#{in_type}], midSetter : TupleSetter[R], midConv : TupleConverter[R], endSetter : TupleSetter[#{out_type}], monR : Monoid[R]" }
mapplus_map_base_call = "(mapfn)(mapfn2)(startConv, midSetter, midConv, endSetter, monR)"

simple_tuple = lambda { |type| "conv : TupleConverter[#{type}]" }

fold_left_fn_builder = lambda { |type| "(init : R)(fn : (R,#{type}) => R)" }
fold_left_tuple = lambda { |type| "setter : TupleSetter[R], conv : TupleConverter[#{type}]" }
fold_left_base_call = "(init)(fn)(setter, conv)"

boolean_fn_builder = lambda { |type| "(fn : (#{type}) => Boolean)" }
boolean_pair_fn_builder = lambda { |type| "(fn : (#{type},#{type}) => Boolean)" }
empty_fn_builder = lambda { |type| "" }

reduce_fn_builder = lambda { |type| "(fn : (#{type},#{type}) => #{type})" }
reduce_tuple_builder = lambda { |type| "setter : TupleSetter[#{type}], conv : TupleConverter[#{type}]" }
reduce_base_call = "(fn)(setter, conv)"

monoid_tuple_builder = lambda { |type| "monoid : Monoid[#{type}], conv : TupleConverter[#{type}], setter : TupleSetter[#{type}]" }
monoid_base_call = "(monoid, conv, setter)"

ring_tuple_builder = lambda { |type| "ring : Ring[#{type}], conv : TupleConverter[#{type}], setter : TupleSetter[#{type}]" }
ring_base_call = "(ring, conv, setter)"

puts "// following were autogenerated by #{__FILE__} at #{Time.now} do not edit"
puts %Q|package com.twitter.scalding

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.algebird.{Monoid, Ring}

trait GeneratedRichPipeOperations extends FieldConversions \{

|

make_maps('map', map_fn_builder, "", map_tuple, map_base_call, "Pipe")
make_maps('mapTo', map_fn_builder, "", map_tuple, map_base_call, "Pipe")
make_maps('flatMap', flat_map_fn_builder, "", map_tuple, map_base_call, "Pipe")
make_maps('flatMapTo', flat_map_fn_builder, "", map_tuple, map_base_call, "Pipe")
make_filters('filter', "Pipe")

puts %Q|\}

trait GeneratedReduceOperations[Self <: GeneratedReduceOperations[Self]] extends FieldConversions \{

|

make_maps('mapReduceMap', mapred_map_fn_builder, "R,", mapred_map_tuple, mapred_map_base_call, "Self")
make_maps('mapPlusMap', mapplus_map_fn_builder, "R,", mapplus_map_tuple, mapplus_map_base_call, "Self")
# TODO: mapList
make_unary_ops('count', boolean_fn_builder, simple_tuple, "", false, lambda { |type| "Long" }, "Self", "(fn)")
make_unary_ops('forall', boolean_fn_builder, simple_tuple, "", false, lambda { |type| "Boolean" }, "Self", "(fn)")
make_reduce_ops('reduce', reduce_fn_builder, reduce_tuple_builder, "Self", reduce_base_call)
make_reduce_ops('plus', empty_fn_builder, monoid_tuple_builder, "Self", monoid_base_call)
make_reduce_ops('times', empty_fn_builder, ring_tuple_builder, "Self", ring_base_call)
make_unary_ops('toList', empty_fn_builder, simple_tuple, "", false, lambda { |type| "List[#{type}]" }, "Self", "")
# TODO: dot
make_unary_ops('sortWithTake', boolean_pair_fn_builder, simple_tuple, "", true, lambda { |type| "List[#{type}]" }, "Self", "(fn)")
# sortedTake and sortedReverseTake don't need to be included since their type params are used only for tuple conversion

puts %Q|\}

trait GeneratedFoldOperations[Self <: GeneratedFoldOperations[Self]] extends GeneratedReduceOperations[Self] \{

|

make_unary_ops('foldLeft', fold_left_fn_builder, fold_left_tuple, "R,", false, lambda { |type| "R" }, "Self", fold_left_base_call)

puts %Q|\}

trait GeneratedStreamOperations[Self <: GeneratedStreamOperations[Self]] extends FieldConversions \{

|

# TODO: mapStream
make_filters('dropWhile', "Self")
make_unary_ops('scanLeft', fold_left_fn_builder, fold_left_tuple, "R,", false, lambda { |type| "R" }, "Self", fold_left_base_call)
make_filters('takeWhile', "Self")

puts %Q|\}

sealed case class Arity private[scalding](arity: Int)
|

(1..$n).each { |arity| puts "sealed class Arity#{arity} private[scalding] extends Arity(#{arity})" }

puts %Q|
trait Arities \{

|

(1..$n).each { |arity| puts "  implicit val arity#{arity} = new Arity#{arity}" }

puts %Q|
\}
|

puts "// end of autogenerated"
