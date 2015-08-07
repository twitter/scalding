# Type-safe API Reference

There are two main concepts in the type-safe API: a `TypedPipe[T]` which is kind of a distributed list of objects of type `T` and a `KeyedList[K,V]` which represents some sharding of objects of key `K` and value `V`.  There are a few KeyedList objects: `Grouped[K,V]`, `CoGrouped[K, V]`.  The former represents usual groupings, and the latter is used for cogroupings or joins.

## Basics

Most of the Typed API is available simply by importing `com.twitter.scalding._`. Most sources, even the simple `TextLine` source, are typed (implement the `TypedSource` trait), which means it is easy to get a `TypedPipe` to begin performing operations on.

```tut:silent
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
```

```tut
val lines: TypedPipe[String] = TypedPipe.from(TextLine("hello.txt"))
```

```tut:fail
// do word count
(lines.flatMap(_.split("\\s+"))
  .group
  .sum
  .write(TypedTsv[(String, Long)]("output")))
```
The above example generated an error.
The problem appears to be that you are running the group function on a TypedPipe[String] when it expects a TypedPipe[(K,V)]. Essentially you need a pipe of tuples in order to group.

```tut
// reverse all lines in the file
val reversedLines: TypedPipe[String] = lines.map(_.reverse)
reversedLines.write(TypedTsv[String]("output.tsv"))
```
In the above example we show the preferred way to get a `TypedPipe` — using `TypedPipe.from()`, and then demonstrate running a map operation and writing out to a typed sink (`TypedTsv`).

## Map-like functions

### map

```scala
def map[U](f : T => U) : TypedPipe[U]
```

Converts a `TypedPipe[T]` to a `TypedPipe[U]` via `f : T => U`

```tut:silent
case class Bird(name: String, weightInPounds: Double,
                heightInFeet: Double, color: String)

val birds: TypedPipe[Bird] = TypedPipe.from(Seq(
    Bird("George", 12.2, 2.1, "blue"),
    Bird("Gatz", 12.9, 3.21, "green"),
    Bird("Jay", 13.9, 2.7, "yellow")))

val britishBirds: TypedPipe[(Double, Double)] =
  birds.map { bird =>
    (0.454 * bird.weightInPounds, 0.305 * bird.heightInFeet)
  }
```

```tut
britishBirds.dump
```

### flatMap

```scala
def flatMap[U](f : T => Iterable[U]) : TypedPipe[U]
```

Converts a `TypedPipe[T]` to a `TypedPipe[U]`
by applying `f : T => Iterable[U]` followed by flattening.

```tut:silent
case class Book(title: String, author: String, text: String)
val books: TypedPipe[Book] = TypedPipe.from(Seq(
  Book("To Kill a Mockingbird", "Harper Lee", "Atticus Finch"),
  Book("The Fountainhead", "Ayn Rand", "Gale Winand"),
  Book("A Separate Peace", "John Knowles", "Finny")))
val words: TypedPipe[String] = books.flatMap { _.text.split("\\s+") }
```

```tut
words.dump
```

Here's an example that uses Option. (Either an animal name passes by in the pipe or nothing.)
```tut
birds.flatMap { b =>
  if (b.color == "yellow") { Some(b.name) } else None
}.dump
```

### filter

```scala
def filter(f: T => Boolean): TypedPipe[T]
```

If you return `true` you keep the row, otherwise the row is ignored.

```tut:silent
case class Animal(name: String, kind: String)
val animals: TypedPipe[Animal] = TypedPipe.from(Seq(
  Animal("George", "rabbit"),
  Animal("Gatz", "bird"),
  Animal("Joe", "cow"),
  Animal("Jay", "bird")))
val birds = animals.filter { _.kind == "bird" }
```

```tut
birds.dump
```

### filterNot

```scala
def filterNot(f : T => Boolean) : TypedPipe[T]
```

Acts like `filter` with a negated predicate - keeps the rows where the predicate function returns `false`, otherwise the row is ignored.

```tut:silent
val notBirds = animals.filterNot { _.kind == "bird" }
```
```tut
notBirds.dump
```

### collect

```scala
def collect(f: PartialFunction[T, U]): TypedPipe[U]
```

Filters and maps with Scala's partial function syntax (case):

```tut:silent
val birdNames: TypedPipe[String] = animals.collect { case Animal(name, "bird") => name }
//This is the same as flatMapping an Option.
```

```tut
birdNames.dump
```

## Creating Groups and Joining (CoGrouping)

### Grouping

These are all methods on `TypedPipe[T]`. Notice that these methods do not return a `TypedPipe[T]` anymore; instead, they return `Grouped[K,T]`.

### groupBy
```scala
def groupBy[K](g: T => K)(implicit ord: Ordering[K]) : Grouped[K,T]
```

Call `g : T => K` on a `TypedPipe[T]` to create a `Grouped[K,T]`.
Subsequent aggregation methods use `K` as the type of the grouping key. We can use any of the functions on Groups specified on the Fields API to transform the `Grouped[K, T]` to a `TypedPipe[U]`. Notice that those functions act on `T`.

Groups need an `Ordering` (i.e. a comparator) for the key `K` that we are grouping by. This is implemented for all the standard variable types that we use, in which case no explicit declaration is necessary.

```tut:silent
case class Book(title: String, author: String, year: Int)
val books: TypedPipe[Book] = TypedPipe.from(Seq(
  Book("To Kill a Mockingbird", "Harper Lee", 1960),
  Book("Go Set a Watchman", "Harper Lee", 2015),
  Book("The Fountainhead", "Ayn Rand", 1943),
  Book("Atlas Shrugged", "Ayn Rand", 1957),
  Book("A Separate Peace", "John Knowles", 1959)))
```
We want to group all the books based on their author
```tut:silent
val byAuthor: Grouped[String, Book] = books.groupBy { case book => book.author }
```

Now, we have `Grouped[String, Book]`.
```tut
byAuthor.dump
```

```tut
byAuthor.size.dump
```
This creates a `KeyedList[String, Int]`, where the String corresponds to the author and the Int corresponds to the number of books that the author wrote. `KeyedList` objects are automatically converted to `TypedPipe`s as needed, or you can call `.toTypedPipe` if you prefer.

### group (implicit grouping)
```scala
// uses scala's <:< to require that T is a subclass of (K, V).
def group[K, V](implicit ev: T <:< (K, V), ord : Ordering[K]): Grouped[K, V]
```

Special case of `groupBy` that can be called on `TypedPipe[(K, V)]`. Uses `K` as the grouping key.

### groupAll (send everything to one reducer)
In scala there is a type that has one less value than Boolean, and that is Unit. There is only value in the type Unit. The value is written as `()`.
```scala
def groupAll: Grouped[Unit,T]
```

Uses `Unit` as the grouping key. Useful to send all tuples to 1 reducer.

Useful functions on `Grouped[K,V]`.
```scala
val group: Grouped[K, V]
group.keys
//Creates a TypedPipe[K] consisting of the keys in the (key, value) pairs of group.
group.values
//Creates a TypedPipe[V] consisting of the values in the (key, value) pairs of group.
group.mapValues { values => mappingFunction(values) }
//Creates a Grouped[K, V'], where the keys in the (key, value) pairs of group are unchanged, but the values are changed to V'.
```

## Joining/CoGrouping
These are all methods on `CoGroupable[K, V]`. `TypedPipe[K, V]`, `Grouped[K, V]` and even `CoGrouped[K, V]` are CoGroupable. If possible, put the CoGroupable with the most values per key on the left; this greatly improves performance, but correctness is not impacted. In extreme cases failure to do so can lead to OutOfMemoryErrors. First, we group the pipe by key of type `K` to get `Grouped[K, V]`. Then, we join with another group of the same key `K`, for example `Grouped[K, W]`.

### join (inner-join)
```scala
def join[W](smaller: CoGroupable[K, W]): CoGrouped[K, (V, W)]
```
Note that `CoGrouped` extends `KeyedListLike`, so any reducing functions you are used to on `Grouped` will also work on a `CoGrouped`.

We already know `K` and `V`. The only type that could be specified in the join function is `W`, which is the value in the key-valued group of the `smaller` group.

Suppose we have two libraries and we want to get a list of the books they have in common.
The books of Library 2 have an additional field "copies."
```tut:silent
import com.twitter.scalding.typed.{CoGroupable, CoGrouped}

case class Book(title: String, author: String)
case class ExtendedBook(title: String, author: String, copies: Long)

val library1: TypedPipe[Book] = TypedPipe.from(Seq(
  Book("To Kill a Mockingbird", "Harper Lee"),
  Book("The Fountainhead", "Ayn Rand"),
  Book("Atlas Shrugged", "Ayn Rand"),
  Book("A Separate Peace", "John Knowles")))

val library2: TypedPipe[ExtendedBook] = TypedPipe.from(Seq(
  ExtendedBook("To Kill a Mockingbird", "Harper Lee", 10),
  ExtendedBook("The Fountainhead", "Ayn Rand", 6),
  ExtendedBook("Go Set a Watchman", "Harper Lee", 2)))

// Group the books of Library 1 by book title.
val group1: CoGroupable[String, Book] = library1.groupBy { _.title }

// Similarly, group the books of Library 2 by book title.
val group2: CoGroupable[String, ExtendedBook] = library2.groupBy { _.title }

// We do group1.join(group2) instead of group2.join(group1)
// because group1 is larger
val theJoin: CoGrouped[String, (Book, ExtendedBook)] = group1.join(group2)
```

```tut
theJoin.dump
```

### leftJoin
```scala
def leftJoin[W](smaller: CoGroupable[K, W]): CoGrouped[K, (V, Option[W])]
```
Using the definitions from the previous example, assume you are the general manager of Library 1 and you are interested in a complete list of all the books in your library. In addition, you would like to know, which of those books can also be found in Library 2, in case the ones in your library are being used:
```tut:silent
val theLeftJoin: CoGrouped[String, (Book, Option[ExtendedBook])] = group1.leftJoin(group2)
```

```tut
theLeftJoin.dump
```

### rightJoin
```scala
def rightJoin[W](smaller: CoGroupable[K, W]): CoGrouped[K, (Option[V], W)]
```

```tut:silent
val theRightJoin: CoGrouped[String, (Option[Book], ExtendedBook)] = group1.rightJoin(group2)
```

```tut
theRightJoin.dump
```

### outerJoin
```scala
def outerJoin[W](smaller: CoGroupable[K, W]): CoGrouped[K, (Option[V], Option[W])]
```

```tut:silent
val theOuterJoin: CoGrouped[String, (Option[Book], Option[ExtendedBook])] = group1.outerJoin(group2)
```

```tut
theOuterJoin.dump
```

Like all KeyedListLike instances, `CoGrouped` has `toTypedPipe` to explicitly convert to TypedPipe. However, this is automatic (implicit from `KeyedListLike[K, V, _] => TypedPipe[(K, V)]` in object KeyedListLike).
```scala
val myJoin: CoGrouped[K, (V, W)]
val tpipe: TypedPipe[(K, (V, W))] = myJoin.toTypedPipe
```

### Joining multiple streams
Since `CoGrouped` is `CoGroupable` it is perfectly legal to do `a.join(b).join(c).leftJoin(d).outerJoin(e)` and it will run in one map/reduce job, but the value type will be a bit ugly:
`(Option[(((A, B), C), Option[D])], Option[E])`. To make this cleaner, in scalding 0.12 we introduce the `MultiJoin` object. `MultiJoin(a, b, c, d)` does an inner join with a value tuple of `(A, B, C, D)` as you might expect. You can also do `MultiJoin.left` or `MultiJoin.outer`.

### Map-side (replicated) joins:
These methods do not require a reduce step, but should only be used on extremely small arguments since each mapper will read the entire argument to do the join.

### cross

Suppose we want to send every value from one `TypedPipe[U]` to each value of a `TypedPipe[T]`. `List(1,2,3) cross List(4,5)` gives `List((1,4),(1,5),(2,4),(2,5),(3,4),(3,5))`.  The final size is `left.size * right.size`.

```scala
// Implements a cross product.  The right side should be tiny.
def cross[U](tiny: TypedPipe[U]): TypedPipe[(T,U)]
```

### hashJoin

A very efficient join, which works when the right side is tiny, is hashJoin. All the (key, value) pairs from the right side are stored in a hash table for quick retrieval. The hash table is replicated on every mapper and the hashJoin operation takes place entirely on the mappers (no reducers involved).
```scala
// Again, the right side should be tiny.
def hashJoin[W](tiny: HashJoinable[K, W]): TypedPipe[(K, (V, W))]
```


> Tip: All groups and joins have .withReducers(n) to explicitly set the number of reducers for that step.
> For other options, please refer to:
> [http://twitter.github.io/scalding/#com.twitter.scalding.typed.Grouped](http://twitter.github.io/scalding/#com.twitter.scalding.typed.Grouped)
> and
> [http://twitter.github.io/scalding/#com.twitter.scalding.typed.CoGrouped](http://twitter.github.io/scalding/#com.twitter.scalding.typed.CoGrouped)

## ValuePipe: working with values that will be computed

Sometimes we reduce everything down to one value:
```scala
val userFollowers: TypedPipe[(Long, Int)] = // function to get

val topUsers: TypedPipe[Long] = allUsers
  .collect { case (uid, followers) if followers > 1000000 => uid }

// put it in a value:
val topUsers: ValuePipe[Set[Long]] = topUsers.map(Set(_)).sum
```
A value Pipe is a kind of future value: it is a value that will be computed by your job, but is not there yet. TypedPipe.sum returns a ValuePipe.

When you have this, you can then use it on another TypedPipe:
```scala
val allClickers: TypedPipe[Long] = //...

val topClickers = allClickers.filterWithValue(topUsers) { (clicker, optSet) =>
  optSet.get.contains(clicker) // keep the topUsers that are also clickers
}
```
You can also mapWithValue or flatMapWithValue.  See [ValuePipe.scala](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/typed/ValuePipe.scala) for more.

## Records
Suppose you have many fields and you want to update just one or two. Did you know about the `copy` method on all case classes?

Consider this example:
```scala
scala> case class Record(name: String, weight: Double)
defined class Record

scala> List(Record("Bob", 180.3), Record("Lisa", 154.3))
res22: List[Record] = List(Record(Bob,180.3), Record(Lisa,154.3))

scala> List(Record("Bob", 180.3), Record("Lisa", 154.3)).map { r =>
  val w = r.weight + 10.0
  r.copy(weight = w)
}
res23: List[Record] = List(Record(Bob,190.3), Record(Lisa,164.3))
```

In exactly the same way, you can update just one or two fields in a case class on scalding with the typed API.

This is how we recommend making records, but WATCH OUT: you need to define case classes OUTSIDE of your job due to serialization reasons (otherwise they create circular references).

## Aggregation and Stream Processing
Both `Grouped[K, R]` and `CoGrouped[K, R]` extend `KeyedListLike[K, R, _]`, which is the class that represents sublists of `R` sharded by `K`. The following methods are the main aggregations or stream processes you can run.

### sum: Generalized reduction

```scala
def sum[U >: V](implicit s: Semigroup[U]): KeyedListLike[K, U]
```

Scalding uses a type from Algebird called a Semigroup for sums. A semigroup is just a reduce function that has the property that `plus(plus(a, b), c) == plus(a, plus(b, c))`. The default Semigroup is what you probably expect: addition for numbers, union for sets, concatenation for lists, maps do an outer join on their keys and then do the semigroup for their value types.

If there is no sorting on the values, scalding assumes that order does not matter and it will partially apply the sum on the mappers. This can dramatically reduce the communication cost of the job depending on how many keys there are in your data set.

### reduce: an ad-hoc Semigroup
```scala
def reduce(fn: (V, V) => V): KeyedListLike[K, V]
```
This defines the plus function for a Semigroup, and then calls sum with that Semigroup. See the documentation there.

### aggregate: Using Aggregators for reusablity
```scala
def aggregate[B,C](a: Aggregator[V, B, C]): KeyedListLike[K, C]
```
check the [aggregator](https://github.com/twitter/scalding/wiki/Aggregation-using-Algebird-Aggregators) tutorial for more explanation and examples.

### foldLeft and fold
```scala
def foldLeft[U](init: U)(fn: (U, V) => U): KeyedListLike[K, U]
```
foldLeft is used where you might make a loop in some language. `U` is the some state you are updating every time you see a new value `V`.  An example might be training a model on some data. U is your model. V are you data points. Your fn looks like:  `foldLeft(defaultModel) { (model, data) => updateModel(model, data) }`.

```scala
def fold[U](f: Fold[V, U]): KeyedListLike[K, U]
def foldWithKey[U](fn: K => Fold[V, U]): KeyedListLike[K, U]
```
A `com.twitter.algebird.Fold` is an instance that encapsulates a fold function. The value of this is two fold:

1. Logic can be packaged in a Fold and shared across many jobs, for instance [`Fold.size`](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Fold.scala#L328)
2. Folds can be combined together so many functions can be applied in one pass over the data.

```tut:silent
import com.twitter.algebird.Fold
val myWork: Fold[Int, (Long, Boolean, Int)] = {
    Fold.size
       .join(Fold.forall { i: Int => i > 0 })
       .join(Fold.sum[Int])
       .map { case ((size, pos), sum) => (size, pos, sum) }
}
```
Folds are similar to Aggregators, with the exception that they MUST be run only on the reducers. If you can express an aggregation in terms of Aggregators, it is worthwhile to do so in that it can give you map-side reduction before going to the reducers.

### mapGroup and mapValueStream: totally general reducer functions

```scala
def mapGroup[U](fn: (K, Iterator[V]) => Iterator[U]): KeyedListLike[K, U]
def mapValueStream[U](fn: Iterator[V] => Iterator[U]): KeyedListLike[K, U]
```
It is pretty rare that you need a reduction that is not a sum, aggregate or fold, but it might occasionally come up. If you find yourself reaching for this very often, it might be a sign that you have not quite grokked how to use Aggregators or Folds.

These functions give you an Iterator over the values on your reducer, and in the case of mapGroup the key, and you can transform just the values, not the key. If you need to change the key, output the new key and value in the U type, and then discard the keys using the `.values` method.

Using mapGroup/mapValueStream always forces all the data to reducers.  Realizing the entire stream of values at once (i.e. manually reversing or rescanning the data) can explode the memory, so prefer to operate one at time on the Iterators you are given.

### Example: Datacubing

A common pattern is called data-cubing. This is where you have some commutative sum that you want to materialize sums of all possible binary queries where part of the key is present or absent (making each point of the key space into a hyper-cube). Here is an [example of how to do this with the typed-API:](https://gist.github.com/johnynek/dfa319c55934b8a38524)

The [[Fields-based API Reference]] has a builder-pattern object called GroupBuilder which allows you to easily create a tuple of
several parallel aggregations, e.g. counting, summing, and taking the max, all in one pass through the data.  The type-safe
API has a way to do this, but it involves implementing a type-class for your object and using `KeyedList.sum` on the tuple.  Below we give an example.

```tut:silent
import com.twitter.algebird.Monoid

case class Hipster(name: String, ridesFixie: Boolean, rimmedGlasses: Boolean, income: Double) {
  def hipsterScore: Double = List(ridesFixie, rimmedGlasses).map { if(_) 1.0 else 0.0 }.sum + 1.0/income
}

// Monoid which chooses the highest hipster score
implicit val hipsterMonoid = new Monoid[Hipster] {
  def zero = Hipster("zeroHipster", false, false, Double.NegativeInfinity)
  def plus(left: Hipster, right: Hipster) =
    List(left, right).maxBy { _.hipsterScore }
}

// Now let's count our fixie riders find the biggest hipster
val people: TypedPipe[Hipster] = TypedPipe.from(Seq(
  Hipster("Joe", true, false, 20),
  Hipster("George", false, false, 100),
  Hipster("Grok", true, true, 1)))

// Now we want to know how many total people, fixie riders, and how many rimmed-glasses wearers,
// as well as the biggest hipster:
val (totalPeople, fixieRiders, rimmedGlasses, biggestHipster) = {
  people.map { person =>
    (1L, if(person.ridesFixie) 1L else 0L, if(person.rimmedGlasses) 1L else 0L, person)
  }.sum
   .toOption
   .get
}
```

```tut
totalPeople
fixieRiders
rimmedGlasses
biggestHipster
```
Scalding automatically knows how to sum tuples (it does so element-wise, see [GeneratedAbstractAlgebra.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/GeneratedAbstractAlgebra.scala).

## Powerful Aggregation with Algebird
[See this example on Locality Sensitive Hashing](https://gist.github.com/azymnis/7940080) via [@argyris](https://twitter.com/argyris/status/411364441909776386).

## Interoperating between Fields API and Type-safe API
If you can avoid the Fields API, we recommend it. But if you have legacy code that you want to keep while you are migrating to the Type-safe API, there are methods to help you.

Generally, all the methods from the [[Fields-based API Reference]] are present with the following exceptions:

1. The mapping functions always replace the input with the output.
map and flatMap in the Type safe API are similar to the mapTo and flatMapTo functions (respectively) in the Fields-based API.
2. Due to the previous statement, there is no need to name fields.

If you `import TDsl._` you get an enrichment on cascading Pipe objects to jump into a Typed block:

```scala
  pipe.typed(('in0, 'in1) -> 'out) { tpipe : TypedPipe[(Int,Int)] =>
     tpipe.groupBy { x => 1 } //groups on all the input tuples (equivalent to groupAll)
     .mapValues { tup => tup._1 + tup._2 } //sum the two values in each tuple
     .sum //sum all the tuple sums (i.e. sum everything)
     .values // discard the key which is 1
  }
```

In this example, we start off with a cascading Pipe (pipe), which has the `'in0` and `'in1` fields. We use the method `typed` in order to create a new TypedPipe (tpipe). Then, we apply all of our functions on the TypedPipe[(Int, Int)] to obtain a TypedPipe[Int] which has the total sum. Finally, this is converted back into the cascading Pipe (pipe) with the single field `'out`, which contains a single Tuple holding the total sum.

**Converting pipes**

* To go from a pipe to a TypedPipe[T]: `mypipe.toTypedPipe[T](Fields_Kept)`.
Fields_Kept specifies the fields in mypipe that we want to keep in the Typed Pipe.
* To go from a TypedPipe[T] to a pipe: `myTypedPipe.toPipe(f: Fields)` method.
Since we go from a Typed to a cascading pipe, we actually need to give names to the fields.

Example:
```scala
import TDsl._

case class Bird(name: String, winLb: Float, color: String)
val birds: TypedPipe[Bird] = getBirdPipe
birds.toPipe('name, 'winLb, 'color) //Cascading Pipe with the 3 specified fields.
birds.toTypedPipe[(String, String)]('name, 'color) //Typed Pipe (keeping only some fields)
```
Advanced examples:
```scala
import TDsl._

case class Bird(name: String, winLb: Float, hinFt: Float, color: String)
val birds: TypedPipe[Bird] = getBirdPipe
birds.toPipe('name, 'color)

val p: TypedPipe[(Double, Double)] =
  TypedTsv[(Double,Double)](input, ('a, 'b))
    .toTypedPipe[(Double, Double)]('a, 'b)
```

`TypedPipe[MyClass]` is slightly more involved, but you can get it in several ways. One straightforward way is:
```scala
object Bird {
  def fromTuple(t: (Double, Double)): Bird = Bird(t._1, t._2)
}

case class Bird(weight: Double, height: Double) {
  def toTuple: (Double, Double) = (weight, height)
}

import TDsl._
val birds: TypedPipe[Bird] =
  TypedTsv[(Double, Double)](path, ('weight, 'height))
    .map{ Bird.fromTuple(_) }
```
