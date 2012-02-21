We highly recommend you go through each of the Tutorials in this directory.  The source
code is full of comments that give the steps.  Once you have completed the tutorial,
read through this document.

Scalding Functions
======================

Learn how to use Scalding functions!

Filter
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

Map
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

Discard, Project
--------------------

Remove columns from your pipe.

```scala
// We can remove fields we don't care about.
val forgetBirth = people.discard('birthplace, 'birthday)

// Discarding is the opposite of projecting.
val keepOnlyWorkplace = people.project('jobTitle, 'salary)
```

Unique
------

Keep only unique rows.

```scala
// Keep only the unique (firstName, lastName) pairs. All other fields are discarded.
people.unique('firstName, 'lastName)
```

MapTo
------

MapTo is equivalent to mapping and then projecting.

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

FlatMap, FlatMapTo
----------------------

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

Limit
-----

Make a pipe smaller.

```scala
// Keep (approximately) 100 rows.
val oneHundredPeople = people.limit(100)
```

GroupBy
==========
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

Turn a column in the group into a list.

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

Like reduce, but all the work happens on the reduce side (so the fold function is not required to be associative, and can in fact output a type different from what it takes in).


count
-------
We can count the number of rows in a group that satisfy some predicate.

```scala
val usersWithImpressions =
  users
    .groupBy('user) { _.count('numImpressions) { x : Long => x > 0 } }
```

GroupAll
---------
There's also a groupAll function, which is useful if you want to (say) count the total number of rows in the pipe.

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

Joins
-------------
We can do inner joins.

joinWithSmaller, joinWithLarger
-------------------------------

```scala
// people is a large pipe with a birth_city_id. We join it with the smaller cities pipe on id.
val peopleWithBirthplaces = people.joinWithSmaller('birth_city_id -> 'id, cities)

// Equivalent to...
val peopleWithBirthplaces = cities.joinWithLarger('id -> 'birth_city_id, people)

// Note that the two pipes can not have a field name in common (not even the field they join on).
// For example, this throws an error:
people.joinWithSmaller('ssn -> 'ssn, teachers)

// Instead, we first rename the ssn field of one of the pipes:
people.rename('ssn -> 'ssnOther).joinWithSmaller('ssnOther -> 'ssn, teachers)
```
