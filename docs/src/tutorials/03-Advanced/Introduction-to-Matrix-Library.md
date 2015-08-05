# Introduction to Matrix Library

### About

Matrix.scala is a Scalding library that introduces the possibility of treating pipes as sparse matrices and to operate on them using standard matrix operations, such as matrix multiplication:
```scala
//Computing the innerproduct of matrix A
innerProd = A * A.transpose
```
The matrix constructor takes in a pipe containing triples that have the assumed semantics of (row index, column index, matrix value). Additionally, the user can specify the approximate dimensions of the matrix (number of rows, columns, non-zero values) and its skewness (if the distribution of values over the row/column keys is skewed or not). This additional information can help speed-up the computation and improve scalability of the resulting job.

### Type restrictions

The matrix row and column indexes can be of any type that is comparable. The usual cases are Int, Long, String. This means that labeled matrices are allowed.
For example, we can create a matrix containing the number of users that like specific movie genres per geo without first reindexing the categorical fields to numerical ids:

```scala
//Loading the number of users interested in movie genres per geo from a Tsv source
val interestsMatrix = Tsv(args("input")).read
   .toMatrix[String,String,Long]('geo, 'movie_genre, 'freq)
```

The value type decides what operations can be applied to the matrix.

* Minimally, in order to support **addition**, the value type T must have the trait `Monoid[T]` (as defined in [algebird/Monoid](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Monoid.scala)).
* In order to support **subtraction**, the value type T must have the trait `Group[T]` ([algebird/Group](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Group.scala)).
* For **multiplication**, the value type T must have the trait `Ring[T]` ([algebird/Ring](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Ring.scala)).
* For **division**, the value type T must have the trait `Field[T]` ([algebird/Field](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Field.scala)).

This approach is more powerful than requiring the value type to be Numeric because it allows all of the four operations (addition, subtraction, multiplication, division) to be extended to non-numeric types. For example:

* String addition can be defined as string concatenation
* List addition can be defined as list concatenation
* Set addition can be defined as set union

These operations can be stacked together: For example, Map addition can be defined as set union and the values in the Maps intersection could be aggregated using their own definition of addition.
By allowing matrix values to be structured types we can work with higher-order tensors such as cubes or four-tensors with the same library.

For more information on algebraic structures, see the following Wikipedia pages:

* [Algebraic structure](http://en.wikipedia.org/wiki/Algebraic_structure)
* [Monoid](http://en.wikipedia.org/wiki/Monoid)
* [Group](http://en.wikipedia.org/wiki/Group_%28mathematics%29)
* [Ring](http://en.wikipedia.org/wiki/Ring_%28mathematics%29)
* [Field](http://en.wikipedia.org/wiki/Field_%28mathematics%29)

# Getting Started. The "Hello World!" example for the Matrix library

## Graph nodes outdegrees

Graphs have a straightforward representation in Matrix library as adjacency matrices. We will use the library to compute the outdegrees of the nodes in the graph.

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class GraphOutDegreeJob(args: Args) extends Job(args) {

  import Matrix._

  val adjacencyMatrix = Tsv(args("input"), ('user1, 'user2, 'rel))
    .read
    .toMatrix[Long,Long,Double]('user1, 'user2, 'rel)

  // each row i represents all of the outgoing edges from i
  // by summing out all of the columns we get the outdegree of i
  adjacencyMatrix.sumColVectors.write(Tsv(args("output")))
}
```

We convert a pipe of triples to a sparse matrix where element[i,j] represents and edge between row[i] and column[j]. We then sum the values of the columns together into a column vector that has the outdegree of node[i] at row[i].


### Next steps

* Read the [Matrix API Reference](https://github.com/twitter/scalding/wiki/Matrix-API-Reference): includes code snippets explaining different kinds of matrix functions (e.g., sumRowVectors, matrix product, element-wise product, diagonal, topRowElems) and much more.
* Go over the [Matrix tutorials](https://github.com/twitter/scalding/tree/master/tutorial): the tutorials range from one-liners to more complex examples that show real applications of the Matrix functions to graph problems and text processing.
