We highly recommend you go through each of the Tutorials in this directory.  The source
code is full of comments that give the steps.  Once you have completed the tutorial,
read through this document.

Scalding Functions
======================

Learn how to use Scalding functions!  They are broken into three sets:
They are broken into three sets: Map-like functions
[RichPipe.scala](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/RichPipe.scala), Grouping/Reducing functions [GroupBuilder.scala](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/GroupBuilder.scala), and Joining operations
[JoinAlgorithms.scala](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/JoinAlgorithms.scala)

Maplike Functions
======================

filter
------

Filter out rows.

```scala
val birds = animals.filter('type) { type : String => type == "Flying" }

// We can also filter on multiple fields at once.
val fastAndTallBirds =
  birds
    .filter('speed, 'height) { x : (Int, Float) =>
      val (speed, height) = x
      (speed > 100) && height > 100
    }
```

map
-----
Add new columns that are functions of the existing ones.

```scala
// Map.
val addSpeedInKm =
  birds
    .map('speed -> 'speedInKm) { speed : Int =>
      val kmPerMile = 1.60944
      speed * kmPerMile
    }
    .rename('speed -> 'speedInMiles)

// We can also map from and to multiple fields at once.
val addMutipleFields =
  birds
    .map(('weightInLbs, 'heightInFt) -> ('weightInKg, 'heightInInches)) {
      x : (Float, Float) =>
      val (weightInLbs, heightInFt) = x
      (0.454 * weightInLbs, 12 * heightInFt)
    }

// Notes:
// Generally, the mapped-to columns get added to the pipe.
// If the mapped-to columns are a subset of the mapped-from columns (meaning that they have the same name), then the original columns get replaced with the new columns.
val foo = bar.map(('a, 'b, 'c) -> ('a, 'b)) { ... } // This works. The new a and b columns replace the old a and b columns.
// However, if the mapped-to columns intersect, but are not a subset of the mapped-from columns, you get an error.
val foo = bar.map(('a, 'b, 'c) -> ('a, 'd)) { ... } // Error!
```

discard, project
--------------------

Remove columns from your pipe.

```scala
// We can remove fields we don't care about.
val forgetBirth = people.discard('birthplace, 'birthday)

// Discarding is the opposite of projecting.
val keepOnlyWorkplace = people.project('jobTitle, 'salary)
```

pack
--------
We can pack multiple fields into a single object, by using Java reflection.
For now this only works for objects that have a default constructor that
takes no arguments.  The Java reflection only happens once for each field,
so the performance should be very good.

For example suppose that you have a class called ```Person```, with
fields ```age``` and ```height```, and setters ```setAge``` and ```setHeight```.
Then you can do the following to populate those fields:

```scala
val people = data.pack[Person](('age, 'height) -> 'person)
```

unpack
--------
Conversely, we can unpack the contents of an object into multiple fields

```scala
val data = people.unpack[Person]('person -> ('age, 'height))
```

The default reflection based unpacker works for case classes, standard Thrift and Protobuf generated
classes.

Defining custom packers and unpackers
-------------------------------------
If you want to use tuple packing and unpacking for objects that do not
depend on Java reflection, then you need to implement the
```TuplePacker``` and ```TupleUnpacker``` abstract classes and define
implicit conversions in the context of your ```Job``` class
[see
TuplePacker.scala](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/TuplePacker.scala)
for more.

mapTo
------

MapTo is equivalent to mapping and then projecting (but is more efficient).

```scala
val savings =
  items
    .mapTo(('price, 'discountedPrice) -> 'savings) {
      x : (Float, Float) =>
      val (price, discountedPrice) = x
      price - discountedPrice
    }

// Equivalent to...
val savingsSame =
  items
    .map(('price, 'discountedPrice) -> 'savings) {
      x : (Float, Float) =>
      val (price, discountedPrice) = x
      price - discountedPrice
    }
    .project('savings)
```

flatMap, flatMapTo
----------------------
flatMap to can be used as a filter and map together.  Your function maps each element to
a list, and then that list is flattened (thus flat-map).

```scala
val words =
  books
    .flatMap('text -> 'word) {
      text : String =>
      text.split("\\s+").map { word : String => word }
    }

// Same as above, but keep only the word column.
val wordsOther =
  books
    .flatMapTo('text -> 'word) {
      text : String =>
      text.split("\\s+").map { word : String => word }
    }
```

limit
-----

Only allows a fixed number of items to pass in a pipe.

```scala
// Keep 100 rows.
val oneHundredPeople = people.limit(100)
```

unique
------
This looks like a mapping function, but it actually requires a map-reduce pair so doing this during
one of your groupBy operations (if you can structure your algorithm to simultaneously do so) will
save work.

Keep only unique rows.

```scala
// Keep only the unique (firstName, lastName) pairs. All other fields are discarded.
people.unique('firstName, 'lastName)
```

Grouping/Reducing Functions
=============================
[see
GroupBuilder.scala](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/GroupBuilder.scala).
Most of these functions were inspired from the [scala.collection.Iterable
API](http://www.scala-lang.org/api/current/scala/collection/Iterable.html).

groupBy
--------
Group your pipe by the values in a specified set of columns, and then apply a grouping function to the values in each group.

```scala
val wordCounts =
  books
    .flatMap('text -> 'word) {
      text : String =>
      text.split("\\s+").map { word : String => word }
    }
    .groupBy('word) {
      // The size function takes the name of the new column.
      // By default, if you don't pass in a new name, the new column is simply called 'size'.
      // Here we call the new column 'count'.
      _.size('count)
    }
    // We now have (word, count) columns in the pipe.
```

Grouping functions include...

size
-----
Count the number of rows in this group

```scala
wordCounts
  .groupBy('word) {
      // By default, if you don't pass in a new name, the new column is simply called 'size'.
      // Here we call the new column 'count'.
      _.size('count)
    }
```

average
-------

Take the mean of a column.

```scala
// Find the mean age of boys vs. girls
people
    .groupBy('sex) {
        // The new column is called 'meanAge'.
        _.average('age -> 'meanAge)
    }
```

mkString
-----------

Turn a column in the group into a string.

```scala
wordCounts
  .groupBy('count) {
    // Take all the words with this count, join them with a comma, and call the new column "words".
    _.mkString('word -> 'words, ",")
  }
```

toList
-------

Turn a column in the group into a list.  An idosyncracy about this is that null items in the list
are removed.  It is equivalent to first filtering null items.  Be careful about depending on this
behavior as it may be changed before scalding 1.0.

```scala
wordCounts
  .groupBy('count) {
    // Take all the words with this count, join them into a list in a new column called "words".
    _.toList[String]('word -> 'words)
  }
```

sum
----
Sum over a column in the group.

```scala
expenses
  .groupBy('shoppingLocation) {
    // Sum over the 'cost' column, and rename the summed column to 'totalCost'.
    _.sum('cost -> 'totalCost)
  }
```

reduce
-------
We can also reduce over grouped columns. This is equivalent to the previous sum.
The reduce function is required to be associative, so that the work can be done on the map side and not solely on the reduce side (like a combiner).

```scala
expenses
  .groupBy('shoppingLocation) {
    _.reduce('cost -> 'totalCost) {
      (costSoFar : Double, cost : Double) => costSoFar + cost
    }
  }
```

foldLeft
-----------

Like reduce, but all the work happens on the reduce side (so the fold function is not required to be
associative, and can in fact output a type different from what it takes in).  Fold is the
fundamental reduce function.


count
-------
We can count the number of rows in a group that satisfy some predicate.

```scala
val usersWithImpressions =
  users
    .groupBy('user) { _.count('numImpressions) { x : Long => x > 0 } }
```

pivot/unpivot
-----------
Pivot/Unpivot are equivalents to SQL/Excel functions that change the data from a row-based
representation to column-based:

```scala
pipe.groupBy('key) { _.pivot(('col, 'val)->('x,'y,'z)) }
```

or using unpivot, from column-based representation to a row-based
(strictly speaking, unpivot is a map-like
function which appears in RichPipe.scala and does not require a reduce-phase):

```scala
pipe.unpivot(('x,'y,'z)->('col,'val))
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

When pivoting, you can provide an explicit default value instead of replacing missing rows with null

```scala
pipe.groupBy('key) {_.pivot(('col, 'val) -> ('x,'y,'z), 0.0) }
```

which will result in 

```
3, 1.2, 3.4, 0.0
4, 0.0, 0.0, 4
```

groupAll
---------
There's also a groupAll function, which is useful if you want to (say) count the total number of rows in the pipe.
Think three times before using this function on Hadoop.  This removes the ability to do any
parallelism in the reducers.  That said, accumulating a global variable may require it.  Tip: if you
need to bring this value to another pipe, try crossWithTiny (another function you should use with
great care).

```scala
// vocabSize is now a pipe with a single entry, containing the total number of words in the vocabulary.
val vocabSize =
  wordCounts
    .groupAll { _.size('vocabSize) }
```

It's also useful if, right before outputting a pipe, you want to sort by certain columns.

```scala
val sortedPeople =
    people.groupAll {
        // Sort by lastName, then by firstName.
        _.sortBy('lastName, 'firstName)
    }
```

Joining Functions
========================

Joins (see JoinAlgorithms.scala)
-------------
All the expected joining modes are present: inner, outer, left, right.  Cascading implements these
as CoGroup operations which are implemented in a single map-reduce job.  It is important to hint the
relative sizes of your data.  There are three main joins: ```joinWithSmaller```,
```joinWithLarger``` and ```joinWithTiny```.  ```joinWithTiny``` is a special join that does not
move the left-hand side from from mappers to reducers.  Instead, all the right hand side is
replicated to the nodes holding the left side.  joinWithTiny is appropriate when you know that 
```Small < N * Big```, where ```N``` is the number of nodes in the job.

When in doubt, choose joinWithSmaller and optimize if that step seems to be taking a very long time.

joinWithSmaller, joinWithLarger
-------------------------------

```scala
// people is a large pipe with a birth_city_id. We join it with the smaller cities pipe on id.
val peopleWithBirthplaces = people.joinWithSmaller('birth_city_id -> 'id, cities)

// Equivalent to...
val peopleWithBirthplaces = cities.joinWithLarger('id -> 'birth_city_id, people)

// Note that the two pipes can only have a field name in common is they are doing an inner join
// on that field. For example, this is allowed:
people.joinWithSmaller('ssn -> 'ssn, teachers)

// But this would fail, ssn field of one of the pipes:
people.joinWithSmaller('ssn -> 'ssn, teachers, joiner=new OuterJoin)
```

