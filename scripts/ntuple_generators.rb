# @author Edwin Chen (@echen)
# Automatically write product monoid, product group, and product ring
# classes for tuples up to size 22.
#
# Run it like this:
#
#   ruby scripts/ntuple_generators.rb > src/main/scala/com/twitter/scalding/mathematics/GeneratedAbstractAlgebra.scala

PACKAGE_NAME = "com.twitter.scalding.mathematics"

# The tuple sizes we want.
TUPLE_SIZES = (3..22).to_a

# Each element in a product tuple is of a certain type.
# This provides an alphabet to draw types from.
TYPE_SYMBOLS = ("A".."Z").to_a

INDENT = "  "

# This returns the comment for each product monoid/group/ring definition.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
#
# Example return:
#   "/**
#    * Combine two monoids into a product monoid
#    */"
def get_comment(n, algebraic_structure)
  ret = <<EOS
/**
* Combine #{n} #{algebraic_structure}s into a product #{algebraic_structure}
*/
EOS
  ret.strip
end

# This returns the class definition for each product monoid/group/ring.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
#
# Example return:
#   "class Tuple2Monoid[T,U](implicit tmonoid : Monoid[T], umonoid : Monoid[U]) extends Monoid[(T,U)]"
def get_class_definition(n, algebraic_structure)
  # Example: "T,U"
  type_values_commaed = TYPE_SYMBOLS.first(n).join(", ")
  "class Tuple#{n}#{algebraic_structure.capitalize}[#{type_values_commaed}](implicit #{get_type_parameters(n, algebraic_structure)}) extends #{algebraic_structure.capitalize}[(#{type_values_commaed})]"
end

# This returns the parameters for each product monoid/group/ring class.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
#
# Example return:
#   "tmonoid : Monoid[T], umonoid : Monoid[U]"
def get_type_parameters(n, algebraic_structure)
  params = TYPE_SYMBOLS.first(n).map{ |t| "#{t.downcase}#{algebraic_structure} : #{algebraic_structure.capitalize}[#{t.upcase}]"}
  params.join(", ")
end

# This returns the method definition for constants in the algebraic structure.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
# constant is "zero", "one", etc.
#
# Example return:
#   "override def zero = (tgroup.zero, ugroup.zero)"
def get_constant(n, algebraic_structure, constant)
  # Example: "tgroup.zero, ugroup.zero"
  constants_commaed = TYPE_SYMBOLS.first(n).map{ |t| "#{t.downcase}#{algebraic_structure}.#{constant}" }.join(", ")
  "override def #{constant} = (#{constants_commaed})"
end

# This returns the method definition for negation in the algebraic structure
# (assuming the structure has an additive inverse).
# n is the size of the product.
# algebraic_structure is "group", "ring", etc.
#
# Example return:
#   "override def negate(v : (T,U)) = (tgroup.negate(v._1), ugroup.negate(v._2))"
def get_negate(n, algebraic_structure)
  negates_commaed = TYPE_SYMBOLS.first(n).each_with_index.map{ |t, i| "#{t.downcase}#{algebraic_structure}.negate(v._#{i+1})" }.join(", ")
  "override def negate(v : (#{TYPE_SYMBOLS.first(n).join(", ")})) = (#{negates_commaed})"
end

# This returns the method definition for associative binary operations in
# the algebraic structure.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
# operation is "plus", "minus", "times", etc.
#
# Example return:
#   "override def plus(l : (T,U), r : (T,U)) = (tmonoid.plus(l._1,r._1), umonoid.plus(l._2, r._2))"
def get_operation(n, algebraic_structure, operation)
  # Example: "(T, U)"
  individual_element_type = "(#{TYPE_SYMBOLS.first(n).join(", ")})"

  # Example: "l : (T, U), r : (T, U)"
  method_params = "l : #{individual_element_type}, r : #{individual_element_type}" # (1..n).to_a.map{ |i| "x#{i}" }.map{ |p| "#{p} : #{individual_element_type}" }.join(", ")

  # Example: "(tmonoid.plus(l._1,r._1), umonoid.plus(l._2, r._2))"
  values_commaed = TYPE_SYMBOLS.first(n).each_with_index.map do |t, i|
    "#{t.downcase}#{algebraic_structure}.#{operation}(l._#{i+1}, r._#{i+1})"
  end.join(", ")
  values_commaed = "(#{values_commaed})"

  "override def #{operation}(#{method_params}) = #{values_commaed}"
end

# Example return:
#   "implicit def pairMonoid[T,U](implicit tg : Monoid[T], ug : Monoid[U]) : Monoid[(T,U)] = {
#    new Tuple2Monoid[T,U]()(tg,ug)
#   }"
def get_implicit_definition(n, algebraic_structure)
  type_params_commaed = get_type_parameters(n, algebraic_structure)

  # Example: "T,U"
  tuple_type_commaed = TYPE_SYMBOLS.first(n).join(", ")

  # Example: "Monoid[(T,U)]"
  return_type = "#{algebraic_structure.capitalize}[(#{tuple_type_commaed})]"

  ret = %Q|#{INDENT}implicit def #{algebraic_structure}#{n}[#{tuple_type_commaed}](implicit #{type_params_commaed}) : #{return_type} = {
#{INDENT}  new Tuple#{n}#{algebraic_structure.capitalize}[#{tuple_type_commaed}]()(#{TYPE_SYMBOLS.first(n).map{ |t| t.downcase + algebraic_structure.downcase }.join(", ")})
#{INDENT}}|
  ret
end

def print_class_definitions
  TUPLE_SIZES.each do |tuple_size|

    code = <<EOS
#{get_comment(tuple_size, "monoid")}
#{get_class_definition(tuple_size, "monoid")} {
  #{get_constant(tuple_size, "monoid", "zero")}
  #{get_operation(tuple_size, "monoid", "plus")}
}

#{get_comment(tuple_size, "group")}
#{get_class_definition(tuple_size, "group")} {
  #{get_constant(tuple_size, "group", "zero")}
  #{get_negate(tuple_size, "group")}
  #{get_operation(tuple_size, "group", "plus")}
  #{get_operation(tuple_size, "group", "minus")}
}

#{get_comment(tuple_size, "ring")}
#{get_class_definition(tuple_size, "ring")} {
  #{get_constant(tuple_size, "ring", "zero")}
  #{get_constant(tuple_size, "ring", "one")}
  #{get_negate(tuple_size, "ring")}
  #{get_operation(tuple_size, "ring", "plus")}
  #{get_operation(tuple_size, "ring", "minus")}
  #{get_operation(tuple_size, "ring", "times")}
}
EOS

    puts code
  end
end

def print_implicit_definitions
  puts "trait GeneratedMonoidImplicits {"
  TUPLE_SIZES.each do |n|
    puts get_implicit_definition(n, "monoid")
    puts
  end
  puts "}"
  puts

  puts "trait GeneratedGroupImplicits {"
  TUPLE_SIZES.each do |n|
    puts get_implicit_definition(n, "group")
    puts
  end
  puts "}"
  puts

  puts "trait GeneratedRingImplicits {"
  TUPLE_SIZES.each do |n|
    puts get_implicit_definition(n, "ring")
    puts
  end
  puts "}"
end

puts "// following were autogenerated by #{__FILE__} at #{Time.now} do not edit"
puts "package #{PACKAGE_NAME}"
puts
print_class_definitions
puts
print_implicit_definitions
