# Fields-based API Reference

Scalding functions can be divided into four types:

* Map-like functions
* Grouping functions
* Reduce operations
* Join operations

## Map-like functions

Map-like functions operate over individual rows in a pipe, usually transforming them in some way. They are defined in [`RichPipe.scala`](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/RichPipe.scala).

### map, flatMap, mapTo, flatMapTo

<a name="map" href="#wiki-map">#</a> pipe.<b>map</b>(<i>existingFields</i> -> <i>additionalFields</i>){<i>function</i>}

Adds new fields that are transformations of existing ones.

```scala
// In addition to the existing `speed` field, the new `fasterBirds`
// pipe will contain a new `doubledSpeed` field (plus any other
// fields that `birds` already contained).
val fasterBirds = birds.map('speed -> 'doubledSpeed) { speed : Int => speed * 2 }
```

You can also map from and to multiple fields at once.

```scala
val britishBirds =
  birds.map(('weightInLbs, 'heightInFt) -> ('weightInKg, 'heightInMeters)) {
    x : (Float, Float) =>
    val (weightInLbs, heightInFt) = x
    (0.454 * weightInLbs, 0.305 * heightInFt)
  }
```
You can map from a field to itself to update its value:
```scala
  items.map('price -> 'price) { price : Float => price * 1.1 }
```
You can use `'*` (here and elsewhere) to mean all fields.

<a name="flatMap" href="#wiki-flatMap">#</a> pipe.<b>flatMap</b>(<i>existingFields</i> -> <i>additionalFields</i>){<i>function</i>}

Maps each element to a list (or an `Option`), and then flattens that list (emits a Cascading Tuple per each item in the returned list).

```scala
val words =
  books.flatMap('text -> 'word) { text : String => text.split("\\s+") }
```
<a name="mapTo" href="#wiki-mapTo">#</a> pipe.<b>mapTo</b>(<i>existingFields</i> -> <i>additionalFields</i>){<i>function</i>}

MapTo is equivalent to mapping and then projecting to the new fields, but is more efficient. Thus, the following two lines produce the same result:

```scala
pipe.mapTo(existingFields -> additionalFields){ ... }
pipe.map(existingFields -> additionalFields){ ... }.project(additionalFields)
```

Here is another example:

```scala
val savings =
  items.mapTo(('price, 'discountedPrice) -> 'savings) {
    x : (Float, Float) =>
    val (price, discountedPrice) = x
    price - discountedPrice
  }
```

```scala
val savingsSame =
  items
    .map(('price, 'discountedPrice) -> 'savings) {
      x : (Float, Float) =>
      val (price, discountedPrice) = x
      price - discountedPrice
    }
    .project('savings)
```

<a name="flatMapTo" href="#wiki-flatMapTo">#</a> pipe.<b>flatMapTo</b>(<i>existingFields</i> -> <i>additionalFields</i>){<i>function</i>}

The `flatMap` analogue of `mapTo`.

```scala
val words =
  books.flatMapTo('text -> 'word) { text : String => text.split("\\s+") }
```

### project, discard

<a name="project" href="#wiki-project">#</a> pipe.<b>project</b>(<i>fields</i>)

Remove all unspecified fields.

```scala
// The new pipe contains only two fields: `jobTitle` and `salary`.
val onlyKeepWorkInfo = people.project('jobTitle, 'salary)
```

<a name="discard" href="#wiki-discard">#</a> pipe.<b>discard</b>(<i>fields</i>)

Removes specified fields. `discard` is the opposite of `project`.

```scala
val forgetBirth = people.discard('birthplace, 'birthday)
```

### insert, rename, limit

<a name="insert" href="#wiki-insert">#</a> pipe.<b>insert</b>(<i>field</i>, <i>value</i>)
Insert field(s) with constant value(s)
```scala
  items.insert(('inflation, 'collegeCostInflation), (0.02, 0.10))
```

<a name="rename" href="#wiki-rename">#</a> pipe.<b>rename</b>(<i>fields</i> -> <i>fields</i>)
Rename fields
```scala
  items.rename(('x, 'y) -> ('X, 'Y))
```

<a name="limit" href="#wiki-limit">#</a> pipe.<b>limit</b>(<i>number</i>)

Allows only a fixed number of items to pass in a pipe.

### filter, filterNot

<a name="filter" href="#wiki-filter">#</a> pipe.<b>filter</b>(<i>fields</i>){<i>function</i>}

Filters out rows for which function is false.

```scala
val birds = animals.filter('type) { type : String => type == "bird" }
```
You can also filter over multiple fields at once.

```scala
val fastAndTallBirds =
  birds.filter('speed, 'height) {
    fields : (Float, Float) =>
    val (speed, height) = fields
    (speed > 100) && (height > 100)
  }
```

<a name="filterNot" href="#wiki-filterNot">#</a> pipe.<b>filterNot</b>(<i>fields</i>){<i>function</i>}

Works exactly like a negated `filter` operation.
It will filter out the rows for which the predicate function returns `true`.

```scala
val notBirds = animals.filterNot('type) { type : String => type == "bird" }
```

### unique

<a name="unique" href="#wiki-unique">#</a> pipe.<b>unique</b>(<i>fields</i>)

Keeps only unique rows based on a specified set of fields.

This looks like a mapping function, but it actually requires a map-reduce pair, so doing this during one of your `groupBy` operations (if you can structure your algorithm to simultaneously do so) will save work.

```scala
// Keep only the unique (firstName, lastName) pairs. All other fields are discarded.
people.unique('firstName, 'lastName)
```

### pack, unpack

<a name="pack" href="#wiki-pack">#</a> pipe.<b>pack</b>(<i>Type</i>)(<i>fields</i> -> <i>object</i>)

You can pack multiple fields into a single object, by using Java reflection. For now this only works for objects that have a default constructor that takes no arguments. The Java reflection only happens once for each field, so the performance should be very good. Basically, the pack and unpack functions are used to group or ungroup fields, respectively, by using Objects.

For example suppose that you have a class called `Person`, with fields `age` and `height`, and setters `setAge` and `setHeight`. Then you can do the following to populate those fields:

```scala
val people = data.pack[Person](('age, 'height) -> 'person)
```

<a name="unpack" href="#wiki-unpack">#</a> pipe.<b>unpack</b>(<i>Type</i>)(<i>object</i> -> <i>fields</i>)

Conversely, you can unpack the contents of an object into multiple fields.

```scala
val data = people.unpack[Person]('person -> ('age, 'height))
```

The default reflection-based unpacker works for case classes as well as standard Thrift- and Protobuf-generated classes.

If you want to use tuple packing and unpacking for objects that do not depend on Java reflection, then you need to implement the `TuplePacker` and `TupleUnpacker` abstract classes and define implicit conversions in the context of your `Job` class.
See [`TuplePacker.scala`](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/TuplePacker.scala)
for more.

## Grouping functions

Grouping/reducing functions operate over <i>groups</i> of rows in a pipe, often aggregating them in some way. They usually involve a reduce phase. These functions are defined in [`GroupBuilder.scala`](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/GroupBuilder.scala).

Most of these functions are inspired by the [scala.collection.Iterable](http://www.scala-lang.org/api/current/scala/collection/Iterable.html) API.

### groupBy, groupAll, groupRandomly, shard

<a name="groupBy" href="#wiki-groupBy">#</a> pipe.<b>groupBy</b>(<i>fields</i>){ <i>group => ...</i> }

Groups your pipe by the values in the specified set of fields, and then applies a set of operations to the group to create a new set of fields. All the entries with the same value (in the field we are grouping by) are sent to the same reducer for processing. But, different values can be sent to different reducers.

```scala
// Create a new pipe with (word, count) fields.
val wordCounts = words.groupBy('word) { group => group.size }
```

Group operations chain together.

```scala
// Create a new pipe containing
// (country, sex, # of people in country of sex, mean age sliced by country and sex).
val demographics = people.groupBy('country, 'sex) { _.size.average('age) }
```
When the field to group by is an enum or a thrift type, currently it won't work properly. Please avoid using enum type for group by.

<a name="groupAll" href="#wiki-groupAll">#</a> pipe.<b>groupAll</b>{ <i>group => ...</i> }

Creates a single group consisting of the entire pipe.

Think three times before using this function on Hadoop. This removes the ability to do any
parallelism in the reducers. That said, accumulating a global variable may require it. Tip: if you
need to bring this value to another pipe, try `crossWithTiny` (another function you should use with
great care).

```scala
// vocabSize is now a pipe with a single entry, containing the total number of words in the vocabulary.
val vocabSize = wordCounts.groupAll { _.size }
```

`groupAll` is also useful if you want to sort a pipe immediately before outputting it.

```scala
val sortedPeople = people.groupAll { _.sortBy('lastName, 'firstName) }
```
As we mentioned earlier, groupBy splits the various groups among different reducers, which do not collaborate. Therefore, if we want to sort everything we use groupAll, which basically sends everything to 1 reducer (since it creates a single group of the entire pipe). Then, the sorting can happen on the reducer.

## Group/Reduce Functions

These are implemented in
[`StreamOperations`](http://twitter.github.io/scalding/#com.twitter.scalding.StreamOperations)
[src](https://github.com/twitter/scalding/blob/master/scalding-core/src/main/scala/com/twitter/scalding/StreamOperations.scala),
[`FoldOperations`]
(http://twitter.github.io/scalding/#com.twitter.scalding.FoldOperations)
[src](https://github.com/twitter/scalding/blob/master/scalding-core/src/main/scala/com/twitter/scalding/FoldOperations.scala),
[`ReduceOperations`]
(http://twitter.github.io/scalding/#com.twitter.scalding.ReduceOperations)
[src](https://github.com/twitter/scalding/blob/master/scalding-core/src/main/scala/com/twitter/scalding/ReduceOperations.scala)

Here is an overview of some of the most popular:

<a name="size" href="#wiki-size">#</a> group.<b>size</b>(<i>name</i>)

Counts the number of rows in this group. By default, the name of the new field is `size`, but you can pass in a new name as well.

```scala
// The new `wordCounts` pipe contains "word" and "size" fields.
val wordCounts = words.groupBy('word) { _.size }

// Same, but calls the new field "count" instead of the default "size".
val wordCounts = words.groupBy('word) { _.size('count) }
```

<a name="average" href="#wiki-average">#</a> group.<b>average</b>(<i>field</i>)

Computes the mean over a field. By default, the new field has the same name as the original field, but you can pass in a new name as well.

```scala
// Find the mean age of boys vs. girls. The new pipe contains "sex" and "age" fields.
val demographics = people.groupBy('sex) { _.average('age) }

// Same, but call the new field "meanAge".
val demographics = people.groupBy('sex) { _.average('age -> 'meanAge) }
```
<a name="sizeAveStdev" href="#wiki-sizeavestdev">#</a> group.<b>sizeAveStdev</b>(<i>field</i>, <i>fields</i>)

Computes the count, average and standard deviation over a field. You must pass new fields to accommodate the output data

```scala
// Find the count of boys vs. girls, their mean age and standard deviation.
// The new pipe contains "sex", "count", "meanAge" and "stdevAge" fields.
val demographics = people.groupBy('sex) { _.sizeAveStdev('age -> ('count, 'meanAge, 'stdevAge) ) }
```

<a name="mkString" href="#wiki-mkString">#</a> group.<b>mkString</b>(<i>field</i>, <i>joiner</i>)

Turns a column in the group into a string. Again, the new field has the same name as the original field by default, but you can also pass in a new name.

```scala
// Take all the words with a given count and join them with a comma.
wordCounts.groupBy('count) { _.mkString('word, ",") }

// Same, but call the new column "words".
wordCounts.groupBy('count) { _.mkString('word -> 'words, ",") }
```

<a name="toList" href="#wiki-toList">#</a> group.<b>toList</b>(<i>field</i>)

Turns a column in the group into a list. An idiosyncracy about this is that null items in the list are removed. It is equivalent to first filtering null items. Be careful about depending on this behavior as it may be changed before scalding 1.0.

```scala
// Take all the words with this count and join them into a list.
wordCounts.groupBy('count) { _.toList[String]('word) }

// Same, but call the new column "words".
wordCounts.groupBy('count) { _.toList[String]('word -> 'words) }
```

<a name="sum" href="#wiki-sum">#</a> group.<b>sum</b>(<i>field</i>)

Sums over a column in the group.

```scala
expenses.groupBy('shoppingLocation) { _.sum[Double]('cost) }

// Same, but call the summed column 'totalCost'.
expenses.groupBy('shoppingLocation) { _.sum[Double]('cost -> 'totalCost) }
```

<a name="max" href="#wiki-max">#</a> group.<b>max</b>(<i>field</i>), group.<b>min</b>(<i>field</i>)

Computes the largest or smallest element of a group.

```scala
expenses.groupBy('shoppingLocation) { _.max('cost) }
```

<a name="count" href="#wiki-count">#</a> group.<b>count</b>(<i>field</i>){<i>function</i>}

Counts the number of rows in a group that satisfy some predicate.

```scala
val usersWithImpressions =
  users
    .groupBy('user) { _.count('numImpressions) { x : Long => x > 0 } }
```

<a name="sortBy" href="#wiki-sortBy">#</a> group.<b>sortBy</b>(<i>fields</i>)

Using **sortBy** you can sort the output before writing it into some output sink.

```scala
users.groupAll { _.sortBy('age) }
```
Note: When reading from a CSV, the data types are set to String,hence the sorting will be alphabetically, therefore to sort by age, an int, you need to convert it to an integer. For example,
```scala
  val users = Csv(file_source, separator = ",", fields = Schema)
    .read
    .map ('age-> 'ageInt) {x:Int => x}
    .groupAll { _.sortBy('ageInt) } // will sort age as a number.
```



<a name="reverse" href="#wiki-reverse">#</a> group.sortBy(<i>fields</i>).<b>reverse</b>

You can also **reverse** the sort-order used (descending, instead of ascending):

```scala
users.groupAll { _.sortBy('age).reverse }
```

At the moment it is a limitation that **reverse** _must_ be called after a **sortBy**, so this: `_.reverse.sortBy('age) /* wrong */` would compile, but would throw an "Cannot sort when reducing" exception during the planning phase.

### reduce, foldLeft

<a name="reduce" href="#wiki-reduce">#</a> group.<b>reduce</b>(<i>field</i>){<i>function</i>}

Applies a reduce function over grouped columns. The reduce function is required to be associative, so that the work can be done on the map side and not solely on the reduce side (like a combiner).

```scala
// This example is equivalent to using `sum`, but you can also supply other reduce functions.
expenses.groupBy('shoppingLocation) {
    _.reduce('cost -> 'totalCost) {
      (costSoFar : Double, cost : Double) => costSoFar + cost
    }
  }
```

<a name="foldLeft" href="#wiki-foldLeft">#</a> group.<b>foldLeft</b>(<i>field</i>){<i>function</i>}

Like reduce, but all the work happens on the reduce side (so the fold function is not required to be associative, and can in fact output a type different from what it takes in). Fold is the fundamental reduce function.

```scala
// for the sake of example, assume we want to discount cost so far by specified amounts
// and that items are in the order we want
expenses.groupBy('shoppingLocation) {
 val init_cost_so_far = 0.0
 _.foldLeft(('cost, 'inflation) -> 'discountedCost)(init_cost_so_far) {
        (discountedCostSoFar: Long, cost_infl: (Double, Double)) =>
        val (cost, inflation) = cost_infl
        discountedCostSoFar * inflation + cost
    }
  }
```

### take & sorting
<a name="take" href="#wiki-take">#</a> group.<b>take</b>(<i>number</i>)

take(n) keeps the first n elements of the group.
```scala
groupBy('shoppingLocation) {
 _.take(100)
}
```
<a name="takeWhile" href="#wiki-takeWhile">#</a> group.<b>takeWhile</b>[T](f : Fields)(fn : (T) => Boolean)

Take while the predicate is true; stop at the first false.

<a name="drop" href="#wiki-drop">#</a> group.<b>drop</b>(<i>number</i>)

drop(n) drops the first n elements of the group.

<a name="sortWithTake" href="#wiki-sortWithTake">#</a> group.<b>sortWithTake</b>(<i> fields -> result_field, take_number</i>)

Equivalent to sorting by a comparison function, then taking k items.  This is MUCH more efficient than doing a total sort followed by a take, since these bounded sorts are done on the mapper, so only a sort of size k is needed.
```scala
sortWithTake( ('clicks, 'tweet) -> 'results, 5) {
  comparison_function : ( clickTweet1 :(Long,Long), clickTweet2:(Long,Long) =>
  clickTweet1._1 < clickTweet2._1 }
```

<a name="sortedReverseTake" href="#wiki-sortedReverseTake">#</a> group.<b>sortedReverseTake</b>[Field Types](fields -> temporary_field_tuple, number)
Reverse stands for decreasing order.
```scala
//calculates top 50 cities and states by number of newborn count.
val TopBirthPlaces=peoplePipe
.groupBy('CityName, 'StateName) { _.size('CountNewBorns) }
.groupAll { _.sortedReverseTake[(Long, String,String)](( 'CountNewBorns, 'CityName,'StateName) -> 'top, 50) }
.flattenTo[(Long,String,String)]('top -> ('CountNewBorns, 'CityName, 'StateName)) //flatenTo as oppose to just flatten to exclude the intermediate top tuple.
}
```
In this example, we first sort by 'CountNewBorns, then by 'CityName and finally by'StateName. Since it is in decreasing order, the entry with most newborns will be the first one. All the fields are stored as a tuple in the 'top field, which we then flatten to get the original fields.

### reducers

<a name="reducers" href="#wiki-reducers">#</a> group.<b>reducers</b>(<i>number</i>)

Override the number of reducers used in the groupBy. Useful when outputting fewer files is desired.
```scala
pipe.groupBy('key) {
    _.sortBy('count).reverse.reducers(6)
}
```
### Chained group operations

Chain together multiple GroupBuilder operations to apply different reductions to different fields:

```scala
group.sum[Long]('x).max('y)
```

### pivot, unpivot

Pivot and unpivot are similar to SQL and Excel functions that change data from a row-based representation to a column-based one (in the case of `pivot`) or vice-versa (in the case of `unpivot`).

<a name="pivot" href="#wiki-pivot">#</a> group.<b>pivot</b>

Converts data from a row-based representation to a column-based one.

```scala
pipe.groupBy('key) { _.pivot(('col, 'val) -> ('x, 'y, 'z)) }
```

In the first example, you need to have rows like:

```
3, "x", 1.2
3, "y", 3.4
4, "z", 4
```

and after the pivot you will have:

```
3, 1.2, 3.4, null
4, null, null, 4
```

When pivoting, you can provide an explicit default value instead of replacing missing rows with null:

```scala
pipe.groupBy('key) { _.pivot(('col, 'val) -> ('x, 'y, 'z), 0.0) }
```

This will result in:

```
3, 1.2, 3.4, 0.0
4, 0.0, 0.0, 4
```

<a name="unpivot" href="#wiki-unpivot">#</a> pipe.<b>unpivot</b>

Converts data from a column-based representation to a row-based one. (Strictly speaking, `unpivot` is a map-like
function which appears in RichPipe.scala and does not require a reduce-phase.)

```scala
pipe.unpivot(('x, 'y, 'z) -> ('col, 'val))
```

## Join operations

Join operations merge two pipes on a specified set of keys, similar to SQL joins. They are defined in [`JoinAlgorithms.scala`](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/JoinAlgorithms.scala).

All the expected joining modes are present: inner, outer, left, and right. Cascading implements these as CoGroup operations which are implemented in a single map-reduce job.

## joins

Since it is important to hint at the relative sizes of your data, Scalding provides three main types of joins. All of them are inner joins:

* `joinWithSmaller`
* `joinWithLarger`
* `joinWithTiny`: this is a special map-side join that does not move the left-hand side from mappers to reducers. Instead, the entire right hand side is replicated to the nodes holding the left side. By, "right hand side," we mean the significantly smaller pipe that we are passing as an argument to this function.

When in doubt, choose `joinWithSmaller` and optimize if that step seems to be taking a very long time.

<a name="joinWithSmaller" href="#wiki-joinWithSmaller">#</a> pipe1.<b>joinWithSmaller</b>(<i>fields</i>, <i>pipe2</i>)

Joins two pipes on a specified set of fields. Use this when `pipe2` has fewer values per key than `pipe1`.

```scala
// `people` is a pipe with a "birthCityId" field.
// It is "larger" because there are many people and many share the same birthCityId
// Join it against the `cities` pipe, which contains an "id" field.
// Cities is "smaller" because it has a smaller number of values per id (in this case 1)
val peopleWithBirthplaces = people.joinWithSmaller('birthCityId -> 'id, cities)

// Join on both city.id and state.id
val peopleWithBirthplaces = people.joinWithSmaller( ('birthCityId , 'birthStateID) -> ('id,'StateID) , cities)
```

<a name="joinWithLarger" href="#wiki-joinWithLarger">#</a> pipe1.<b>joinWithLarger</b>(<i>fields</i>, <i>pipe2</i>)

Joins two pipes on a specified set of fields. Use this when `pipe2` has more values per key than `pipe1`.

```scala
// `cities` is a pipe with an "id" field.
// `cities` is "smaller" because it has a smaller number of values per id (in this case 1)
// Join it against the `people` pipe, which contains a "birthCityId" field.
// `people` is "larger" because there are many people and many share the same birthCityId
val peopleWithBirthplaces = cities.joinWithLarger('id -> 'birthCityId, people)
```

<a name="joinWithTiny" href="#wiki-joinWithTiny">#</a> pipe1.<b>joinWithTiny</b>(<i>fields</i>, <i>pipe2</i>)

Joins two pipes on a specified set of fields. As explained above, this is a special map-side join that does not move the left-hand side from mappers to reducers. Instead, the entire right hand side is replicated to the mappers (nodes) holding the left side. `joinWithTiny` is appropriate when you know that `# of rows in bigger pipe > mappers * # rows in smaller pipe`, where `mappers` is the number of mappers in the job.

```scala
// Assume this is a small pipe containing at most couple thousand rows.
val celebrities = ...

val celebrityBirthplaces = cities.joinWithTiny('id -> 'birthCityId, celebrities)
```

## join modes

By default, all joins are inner joins. You can also specify that you want a <b>left join</b>, a <b>right join</b>, or an <b>outer join</b>.
<b>left join</b>: It keeps all the rows/entries from the left pipe and attaches the entries that have matching keys from the right pipe. The entries of the left pipe that do not have any matches with the right pipe have `null` for the new fields introduced by the right pipe.
<b>right join</b>: Similar to the left join; it keeps all the rows/entries from the right pipe.
<b>outer join</b>: This join keeps all entries from both pipes. Again, if there is no match the empty fields contain `null`.

```scala
import cascading.pipe.joiner._

people.joinWithSmaller('birthCityId -> 'id, cities, joiner = new LeftJoin)
people.joinWithSmaller('birthCityId -> 'id, cities, joiner = new RightJoin)
people.joinWithSmaller('birthCityId -> 'id, cities, joiner = new OuterJoin)
```

Note that when performing an inner join, the left and right pipes are allowed to join on common field names.

```scala
// This is allowed. Only a single "ssn" field will be left in the resulting merged pipe.
people.joinWithSmaller('ssn -> 'ssn, teachers)
// Instead
people.joinWithSmaller('ssn_left -> 'ssn_right, teachers)
// Both fields are kept after the join.
```

However, joining on common field names is not allowed for the left joins, right joins, or outer joins (since it is useful to know whether a missing field value comes from the left pipe or the right pipe).

```scala
// This is not allowed.
people.joinWithSmaller('ssn -> 'ssn, teachers, joiner = new OuterJoin)
```

### crossWithTiny

<a name="crossWithTiny" href="#wiki-crossWithTiny">#</a> pipe1.<b>crossWithTiny</b>(<i>pipe2</i>)

Performs the cross product of two pipes.  The right (pipe2) is replicated to all the nodes, and the left is not moved at all.  Therefore, the "tiny" part should be on the right.existingFields


## Miscellaneous functions
### On pipes
All this and more in [`RichPipe.scala`](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/RichPipe.scala):
* `pipe1 ++ pipe2` to union two pipes that have the same fields
* `p.addTrap` to capture any exceptions thrown on the pipe
```scala
val peopleWithBirthplaces = people.joinWithSmaller('birthCityId -> 'id, cities)
   .addTrap(Tsv("/home/data/error_folder/"))
```
* `p.debug` to see rows on stdout/stderr
* `p.name("myPipe")` to name your pipe
* `p.partition(fields_to_apply_function -> field_based_on_function_output) {function} {group}`
Given a function, it partitions the pipe into several groups based on the output of the function. Then applies a GroupBuilder function on each of the groups.
```scala
pipe.mapTo(()->('age, 'weight) { ... }
    .partition('age -> 'isAdult) { _ > 18 } { _.average('weight) }
//pipe now contains the average weights of adults (above 18) and minors.
```
* `p.sample(percentage)` where 0.00 < percentage < 1.00, note that percentage is actually a decimal
* `p.thenDo{ p : Pipe => if(scala.util.Random.nextInt(2) > 0) p.insert('foo, 1) else p }`
* `p.write(Tsv("myfile"))`

### On groups
All this and more in [`ReduceOperations.scala`](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/ReduceOperations.scala):
* `group.dot('a, 'b, 'a_dot_b)` dot product
```scala
groupBy('x) { _.dot('y,'z, 'ydotz) }
//First do "times" on each pair, then "plus" them all together.
```
* `group.head` Return the first element ; useful mostly for sorted case.
* `group.histogram`
* `group.last` Return the last element; again, useful mostly for sorted case.

More functions can be found at
[`StreamOperations`](http://twitter.github.io/scalding/#com.twitter.scalding.StreamOperations) for stream-like functions (e.g take, drop) and [`FoldOperations`] (http://twitter.github.io/scalding/#com.twitter.scalding.FoldOperations) for fold/reduce-like functions (e.g. foldLeft).

## About functions on multiple fields at once
In many places in the scalding fields-based API can functions be applied to multiple fields at once.
For example:
```scala
val britishBirds =
  birds.map(('weightInLbs, 'heightInFt) -> ('weightInKg, 'heightInMeters)) {
    x : (Float, Float) =>
    val (weightInLbs, heightInFt) = x
    (0.454 * weightInLbs, 0.305 * heightInFt)
  }
```
The parameter x is a tuple of size 2 which is consistent with the number of fields the function is expected to operate on. As an alternative you can also import FunctionImplicits._ and use a a regular function with multiple input arguments:
```scala
import com.twitter.scalding.FunctionImplicits._

val britishBirds =
  birds.map(('weightInLbs, 'heightInFt) -> ('weightInKg, 'heightInMeters)) {
    (weightInLbs: Float, heightInFt: Float) =>
    (0.454 * weightInLbs, 0.305 * heightInFt)
  }
```
