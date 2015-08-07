# Aggregation using Algebird Aggregators

For this tutorial, you need to be using Algebird 0.7.2, 0.8.2 or 0.9 or later. You may need to update your build file (prefer 0.7.2 if you are on scalding 0.11 or scalding 0.12). Scalding 0.13+ comes with algebird 0.9 already.

Aggregators enable creation of reusable and composable aggregation functions. There are three main functions on Aggregator trait.

```scala
trait Aggregator[-A, B, +C]  {
  /**
   * Transform the input before the reduction.
   */
  def prepare(input: A): B
   /**
   * Combine two values to produce a new value.
   */
  def reduce(l: B, r: B): B
  /**
   * Transform the output of the reduction.
   */
  def present(reduction: B): C
}
```

##Examples

In this section we will use the data below to show SQL aggregate functions and how to build similar aggregate functions in Scalding. You can run these in the scalding repo by typing: `./sbt "scalding-repl/run --local"` and then use the `.dump` method to print results (or .get on `ValuePipe`s).

| OrderID | OrderDate  | OrderPrice | OrderQuantity | CustomerName |
|:-------:|:----------:|:----------:|:-------------:|:------------:|
|1	  | 12/22/2005 |    160	    |       2	    |   Smith      |
|2	  | 08/10/2005 |    190	    |       2	    |   Johnson    |
|3	  | 07/13/2005 |    500	    |       5	    |   Baldwin    |
|4	  | 07/15/2005 |    420	    |       2	    |   Smith      |
|5	  | 12/22/2005 |    1000    |	    4	    |   Wood       |
|6	  | 10/2/2005  |    820	    |       4	    |   Smith      |
|7	  | 11/03/2005 |    2000    |	    2	    |   Baldwin    |

```scala
 case class Order(orderId: Int, orderDate: String, orderPrice: Long, orderQuantity: Long,
                  customerName: String)

 val orders = List(
    Order(1, "12/22/2005", 160, 2, "Smith"),
    Order(2, "08/10/2005", 190, 2, "Johnson"),
    Order(3, "07/13/2005", 500, 5, "Baldwin"),
    Order(4, "07/15/2005", 420, 2, "Smith"),
    Order(5, "12/22/2005", 1000, 4, "Wood"),
    Order(6, "10/2/2005", 820, 4, "Smith"),
    Order(7, "11/03/2005", 2000, 2, "Baldwin"))
```

### Count
The SQL COUNT function returns the number of rows in a table satisfying the criteria specified in the WHERE clause.

```sql
SQL:
SELECT COUNT (*) FROM Orders
WHERE CustomerName = 'Smith'
```
```scala
//Scalding:
import com.twitter.algebird.Aggregator.count

TypedPipe.from(orders)
      .aggregate(count(_.customerName == "Smith"))
```
```
Output: 3
```
If you don’t specify a WHERE clause when using COUNT, your statement will simply return the total number of rows in the table

```sql
SQL:
SELECT COUNT(*) FROM Orders
```
```scala
//Scalding:
import com.twitter.algebird.Aggregator.size

TypedPipe.from(orders)
      .aggregate(size)
```

```
Output: 7
```
You can also use aggregate functions with `Group By`.

```sql
SQL:
Select CustomerName, Count(CustomerName)
From Orders
Group by CustomerName
```
```scala
//Scalding:
import com.twitter.algebird.Aggregator.size

TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(size)

Output:
(Baldwin,2)
(Johnson,1)
(Smith,3)
(Wood,1)
```

### Sum
The SQL SUM function is used to return the sum of an expression in a SELECT statement

```sql
SQL:
SELECT SUM(OrderQuantity)
FROM Orders
GROUP BY CustomerName
```
```scala
//Scalding:
import Aggregator.{ prepareMonoid => sumAfter }

TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(sumAfter(_.orderQuantity))

Output:
(Baldwin,7)
(Johnson,2)
(Smith,8)
(Wood,4)
```

### Max
The SQL MAX function retrieves the maximum numeric value from a column.

```sql
SQL:
SELECT CustomerName, MAX(OrderQuantity)
FROM Order
GROUP By CustomerName
```
```scala
//Scalding:
import com.twitter.algebird.Aggregator.max

val maxOp = Aggregator.max[Long].composePrepare { o: Order => o.orderQuantity }

 TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(maxOp)

Output:
(Baldwin,5)
(Johnson,2)
(Smith,4)
(Wood,4)
```

### Min
The SQL MIN function selects the smallest number from a column.

```sql
SQL:
SELECT CustomerName, MIN(OrderQuantity)
FROM Order
GROUP By CustomerName
```
```scala
//Scalding:
import com.twitter.algebird.Aggregator.minBy

 // Rather than using composePrepare, we could also use minBy with andThenPresent:
 val minOp = minBy[Order, Long](_.orderQuantity)
      .andThenPresent(_.orderQuantity)

 TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(minOp)
```

### AVG

The SQL AVG function calculates average value of a numeric column.

```sql
SQL:
SELECT CustomerName, AVG(OrderQuantity)
FROM Order
GROUP BY CustomerName
```

```scala
import com.twitter.algebird._

 val avg = AveragedValue.aggregator.composePrepare[Order](_.orderQuantity)

 TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(avg)

Output:
(Baldwin,3.5)
(Johnson,2.0)
(Smith,2.66)
(Wood,4.0)
```

### Distinct
The SQL DISTINCT function selects distinct values  from a column. In scalding we use a probabilistic data structure called [HyperLogLog](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala) to calculate distinct values.

```sql
SQL:
SELECT DISTINCT CustomerName
FROM Order
```
```scala
//Scalding:
import com.twitter.algebird.HyperLogLogAggregator

val unique = HyperLogLogAggregator
       //HLL Error is about 1.04/sqrt(2^{bits}), so you want something like 12 bits for 1% error
      // which means each HLLInstance is about 2^{12} = 4kb per instance.
      .sizeAggregator(bits = 12)
       //convert customer names to UTF-8 encoded bytes as HyperLogLog expects a byte array.
      .composePrepare[Order](_.customerName.getBytes("UTF-8"))

TypedPipe.from(orders)
      .aggregate(unique)

Output:
4.0
```

### Top K

```scala
 import com.twitter.algebird.Aggregator.sortedReverseTake

 val topK = sortedReverseTake[Long](2)
             .composePrepare[Order](_.orderQuantity)

 TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(topK)

Output:
(Baldwin,List(5, 2))
(Johnson,List(2))
(Smith,List(4, 2))
(Wood,List(4))
```

### Composing Aggregators
Aggregators can be composed to perform multiple aggregation in one pass.

```scala
  import com.twitter.algebird.Aggregator._

  val maxOp = maxBy[Order, Long](_.orderQuantity).andThenPresent(_.orderQuantity)
  val minOp = minBy[Order, Long](_.orderPrice).andThenPresent(_.orderPrice)
  val combinedMetric = maxOp.join(minOp)

  TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(combinedMetric)

Output:
(Baldwin,(5,500))
(Johnson,(2,190))
(Smith,(4,160))
(Wood,(4,1000))
```
composition can also be used to combine two or more aggregators to derive a new aggregate function.

```scala
 import com.twitter.algebird.Aggregator._
 import Aggregator.{ prepareMonoid => sumAfter }

 val sumAggregator = sumAfter[Order, Long](_.orderQuantity)
 val sizeAggregator = size
 /*
   Use more efficient `AveragedValue.aggregator` for AVG calculation. This example
   is only to show how to combine two aggregators.
  */
 val avg = sumAggregator.join(sizeAggregator)
            .andThenPresent{ case (sum, count) => sum.toDouble / count.toDouble }

 TypedPipe.from(orders)
      .groupBy(_.customerName)
      .aggregate(avg)

Output:
(Baldwin,3.5)
(Johnson,2.0)
(Smith,2.66)
(Wood,4.0)
```

you can join up to 22 `aggregators` by using `GeneratedTupleAggregator`. Example below show calculating Max, Min, Sum, Count, Mean and Standard Deviation in one pass by joining different aggregators.

```scala
   import com.twitter.algebird.Aggregator._
   import com.twitter.algebird.{GeneratedTupleAggregator, MomentsAggregator, Moments }
   import Aggregator.{ prepareMonoid => sumAfter }

   val maxOp = maxBy[Order, Long](_.orderPrice)
   val minOp = minBy[Order, Long](_.orderPrice)
   val sum = sumAfter[Order, Long](_.orderPrice)
   val moments = Moments.aggregator.composePrepare[Order](_.orderPrice.toDouble)

   val multiAggregator = GeneratedTupleAggregator
      .from4(maxOp, minOp, sum, moments)
      .andThenPresent {
        case (mmax, mmin, ssum, moment) =>
              (mmax.orderPrice, mmin.orderPrice, ssum, moment.count, moment.mean, moment.stddev)
      }

    TypedPipe.from(orders)
        .groupBy(_.customerName)
        .aggregate(multiAggregator)

Output:
(Baldwin,(2000,500,2500,2,1250.0,750.0))
(Johnson,(190,190,190,1,190.0,0.0))
(Smith,(820,160,1400,3,466.66,271.46))
(Wood,(1000,1000,1000,1,1000.0,0.0))
```
