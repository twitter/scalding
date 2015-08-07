# SQL to Scalding

## Motivation

SQL is a popular language for data analytics. Scalding is a relative newcomer that is more powerful and complex. The goal of this document is to translate commonly used SQL idioms to the Scalding type-safe API (which is preferred over the fields-based API). We are using Vertica SQL variant that is based on PSQL and has support for analytic functions. We have purposely picked trivial example datasets so that it is easy to experiment using the REPL and view intermediate results to get a better understanding of what each method does. More information on how to use the REPL is in [[Scalding REPL]] and [Learning Scalding with Alice](https://gist.github.com/johnynek/a47699caa62f4f38a3e2).

Prerequisites:

* Elementary knowledge of Scala
* Basic ability to decipher types in Scalding methods

You should not expect Scalding to be as intuitive as SQL, but at the same time it is not as hard as it may seem when you see the plethora of classes and methods in the Scalding docs.

To get a deeper understanding of monoids like QTree, please see [Learning Algebird Monoids with REPL](https://github.com/twitter/algebird/wiki/Learning-Algebird-Monoids-with-REPL)

```tut:silent
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
```

## Create datasets
**SQL**
```sql
CREATE TABLE test.allsales(
  state VARCHAR(20),
  name VARCHAR(20),
  sales INT
);

INSERT INTO test.allsales VALUES('CA', 'A', 60);
INSERT INTO test.allsales VALUES('CA', 'A', 20);
INSERT INTO test.allsales VALUES('VA', 'B', 15);
COMMIT;

pwagle=> select * from test.allsales;
 state | name | sales
-------+------+-------
 CA    | A    |    60
 VA    | B    |    15
 CA    | A    |    20
(3 rows)
```
**Scalding**
```tut
case class Sale(state: String, name: String, sale: Int)

val salesList = List(Sale("CA", "A", 60), Sale("CA", "A", 20), Sale("VA", "B", 15))

val salesPipe = TypedPipe.from(salesList)
```

## Simple Count
**SQL**
```sql
pwagle=> select count(1) from test.allsales;
 count
-------
     3
```

**Scalding**
```tut
salesPipe.groupAll.size.values.dump
```

## Count distinct
**SQL**
```sql
pwagle=> select count(distinct state) from test.allsales;
 count
-------
     2
```
**Scalding**
```tut
salesPipe.map{ _.state }.distinct.groupAll.size.values.dump
```

## Count, Count distinct, Sum in one query
**SQL**
```sql
pwagle=> select count(1), count(distinct state), sum(sales) from test.allsales;
 count | count | sum
-------+-------+-----
     3 |     2 |  95
```
**Scalding**
```tut
{
  salesPipe.map{x => (1, Set(x.state), x.sale) }
           .groupAll
           .sum
           .values
           .map{ case(count, set, sum) => (count, set.size, sum) }
           .dump
}
```
The above query will have performance issues if count(distinct state) is large. This can be solved in two ways:

* Group by state first (TODO)
* Using an approximate data structure like HyperLogLog (TODO)

Also see [[Aggregation using Algebird Aggregators]].

## Where
**SQL**
```sql
select state, name, sales
from test.allsales
where
state = 'CA';
```
**Scalding**
```tut
salesPipe.filter(sale => (sale.state == "CA")).dump
```

## Order by X, Y limit N
**SQL**
```sql
select state, name, sale
from test.allsales
order by state, name
limit 1;
```
**Scalding**
```tut:silent
object SaleOrderingWithState extends Ordering[Sale] {
  def compare(a: Sale, b: Sale) = a.state compare b.state
}

implicit val saleOrderingWithState = SaleOrderingWithState
```

```tut
salesPipe.groupAll.sorted.values.dump

salesPipe.groupAll.sorted.take(1).values.dump

salesPipe.groupAll.sortedTake(1).values.dump
```

## Union
**SQL**
```sql
select state, name, sales from test.allsales
UNION ALL
select state, name, sales from test.allsales2
```
**Scalding**
```tut
val salesPipe1 = TypedPipe.from(salesList)

val salesPipe2 = TypedPipe.from(salesList)

(salesPipe1 ++ salesPipe2).dump
```

## Group and Aggregate
**SQL**
```sql
pwagle=> select state, count(1), count(distinct name), sum(sales)
pwagle-> from test.allsales
pwagle-> group by state;
 state | count | count | sum
-------+-------+-------+-----
 CA    |     2 |     1 |  80
 VA    |     1 |     1 |  15
```
**Scalding**
```tut
{
  salesPipe.map{ x => (x.state, (1, Set(x.name), x.sale)) }
           .group
           .sum
           .dump
}

{
  salesPipe.map{ x => (x.state, (1, Set(x.name), x.sale)) }
     .group
     .sum
     .map{ case (state, (count, set, sum)) => (state, (count, set.size, sum))}
     .dump
}
```

## Join
**Scalding**
```tut:silent
case class Table1Row(field1: String, val1: Int)
case class Table2Row(field2: String, val2: Int)

val table1 = TypedPipe.from(List(
    Table1Row("a", 1),
    Table1Row("b", 2)))
val table2 = TypedPipe.from(List(
    Table2Row("b", 3),
    Table2Row("c", 4)))

val table1Group = table1.groupBy { _.field1 }
val table2Group = table2.groupBy { _.field2 }

val join = table1Group.join(table2Group)
```

```tut
join.dump
```

```tut:silent
val leftJoin = table1Group.leftJoin(table2Group)
val outerJoin = table1Group.outerJoin(table2Group)
```

```tut
leftJoin.dump
outerJoin.dump
```

## Histogram, Ntile
**SQL**
```sql
TODO
```
**Scalding Histogram Fields-based Only**
```scala
val inputTp: TypedPipe[Int] = TypedPipe.from(List(5, 2, 3, 3, 4, 4, 4, 1, 15, 30))
val p = inputTp.toPipe(('value))
val p1 = p.groupAll { group => group.histogram('value -> 'histogram) }
  .map('histogram -> ('min, 'q1, 'median, 'q3, 'max, 'mean)) {
     x: Histogram => (x.min, x.q1, x.median, x.q3, x.max, x.mean)
   }
val outputTp = p1.toTypedPipe[(Double, Double, Double, Double, Double, Double)](('min, 'q1, 'median, 'q3, 'max, 'mean))
outputTp.dump
(1.0,3.0,4.0,5.0,30.0,7.1)
```
**Scalding QTree**
```scala

val inputTp: TypedPipe[Int] = TypedPipe.from(List(5, 2, 3, 3, 4, 4, 4, 1, 15, 30))
implicit val qtSemigroup = new QTreeSemigroup[Long](6)
val v = inputTp.map {x => QTree(x)}.groupAll.sum.values

scala> val inputTp: TypedPipe[Int] = TypedPipe.from(List(5, 2, 3, 3, 4, 4, 4, 1, 15, 30))
inputTp: com.twitter.scalding.package.TypedPipe[Int] = IterablePipe(List(5, 2, 3, 3, 4, 4, 4, 1, 15, 30))

scala> val v = inputTp.map {x => QTree(x)}.groupAll.sum.values
<console>:41: error: Cannot find Semigroup type class for com.twitter.algebird.QTree[Long]
       val v = inputTp.map {x => QTree(x)}.groupAll.sum.values
                                                    ^

scala> implicit val qtSemigroup = new QTreeSemigroup[Long](6)
qtSemigroup: com.twitter.algebird.QTreeSemigroup[Long] = com.twitter.algebird.QTreeSemigroup@4e92c2ed

scala> val v = inputTp.map {x => QTree(x)}.groupAll.sum.values
v: com.twitter.scalding.typed.TypedPipe[com.twitter.algebird.QTree[Long]] = com.twitter.scalding.typed.TypedPipeFactory@7925e180

scala> v.map { q => (q.count, q.upperBound, q.lowerBound, q.quantileBounds(.5), q.quantileBounds(.95)) }.dump
(10,32.0,0.0,(4.0,5.0),(15.0,16.0))
```

## TODO

### Analytic / Window Functions (Rank, Ntile, Lag/Lead)
Running Total, Moving Average, Sessionization
