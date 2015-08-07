# Matrix API Reference

Matrix library functions can be divided into three types:

* Value operations
* Vector operations
* Matrix operations

In addition, matrix library contains

* ConversionFrom functions
* ConversionTo functions
* Other functions


## Value operations

Value operations work over individual values in a matrix, usually transforming them in some way.

### mat.mapValues( *mapping function* ): Matrix

Maps the values of the matrix to new values using a function

```scala
// The new matrix contains the elements from the original matrix multiplied by 2
val doubledMatrix = matrix.mapValues{ value : Int => value * 2 }
```

### mat.filterValues( *filter function* ): Matrix

Keep only the values of the matrix that set the function to true

```scala
// The new matrix contains only the positive elements from the matrix
val filteredMatrix = matrix.filterValues{ _ > 0 }
```


### mat.binarizeAs[NewValT]: Matrix

Sets all of the non-zero values of the matrix to the one element of the type

```scala
// The new matrix contains ones as integers for all non-zero values from the matrix
val binMatrix = matrix.binarizeAs[Int]
```

## Vector operations

Vectors are represented as pipes of pairs of indexes and values. There are two classes of vectors, one for row and one for column vectors respectively. Objects from the two classes can convert to one another through `vector.transpose` and to a two-dimensional matrix using `vector.toMatrix` or `vector.diag`.

### RowVector operations

### mat.getRow( rowNumber ): RowVector

Returns the row indexed with the specified value from the original matrix
```scala
// Returns the 3rd row from the matrix
val row = matrix.getRow(3)
// Returns the row from the matrix that is indexed with “France”
val row = matrix.getRow("France")
```

### matrix.reduceRowVectors( *reduce function* ): RowVector

Reduces all row vectors into a single row vector using a associative pairwise aggregation function
```scala
// Produces a row vector containing the products of each column's values
// matrix = 1 1 1
//          2 2 1
//          3 0 1
// rowProd = 6 0 1
val rowProd = matrix.reduceRowVectors { (x,y) => x * y }
```

### matrix.sumRowVectors: RowVector

Reduces all row vectors into the sum row vector
```scala
// Returns the row vector of all per-column sums
//      101   102
//    -------------
//  1 |  1       2
//  2 |          1
//  3 |  3       5

val rowSum = matrix.sumRowVectors

// rowSum (a 1x2 vector) is
//  101  102
// --------
// | 4    8
```

### matrix.mapRows( *mapping function* ): Matrix

Maps each of the row vectors into a new row vector using a function over the list of non-zero elements in the original rows
```scala
// Returns the mean-centered rows of the original matrix
val rowSum = matrix.mapRows{ meanCenter }
def meanCenter[T](vct: Iterable[(T,Double)]) : Iterable[(T,Double)] = {
    val valList = vct.map { _._2 }
    val sum = valList.sum
    val count = valList.size
    val avg = sum / count
    vct.map { tup => (tup._1, tup._2 - avg) }
}
```

### matrix.topRowElems( numberOfElements ): Matrix

Returns the matrix containing only the top K greatest elements in each row as specified by the type-specific comparator. The new matrix pipe has the elements sorted in decreasing order per row key.
```scala
// Returns the matrix with top 10 elements
// Given a matrix:
// matrix =     jim apple       3
                jim orange      6
                jim banana     10
                jim grapefruit  4
                bob apple       7
                bob orange      0
                bob banana      1
                bob grapefruit  3

// topkMatrix = jim banana    10
                jim orange     6
                bob apple      7
                bob grapefruit 3
val topkMatrix = matrix.topRowElems(2)
```

### matrix.rowL1Normalize: Matrix

Returns the matrix containing the L1 row-normalized elements of the original matrix
```scala
// Returns the adjacency matrix of the follow graph normalized by outdegree
// matrix = 1 0 1
//          1 1 1
//          1 0 0
// matrixL1Norm = 0.5 0 0.5
//                0.33 0.33 0.33
//                1 0 0
val matrixL1Norm = matrix.rowL1Normalize
```

### matrix.rowL2Normalize: Matrix

Returns the matrix containing the L2 row-normalized elements of the original matrix
```scala
// Returns the adjacency matrix of the follow graph normalized by outdegree
// matrix = 3 0 4
//          0 0 0
//          2 0 0
// matrixL2Norm = 0.6 0 0.8
//                0   0 0
//                1   0 0
val matrixL2Norm = matrix.rowL2Normalize
```

### matrix.rowMeanCentering: Matrix

Returns the matrix containing the row-mean centered elements of the original matrix, by computing the mean per each row and substracting it from the row elements, such that the original mean value is now zero. (the code is shown above in the mapRows example)
```scala
// Substracts all of the row-wise means from all the elements
val matrixCentered = matrix.rowMeanCentering
```

### matrix.rowSizeAveStdev: Matrix

Computes the row size, average and standard deviation returning a new kx3 matrix where each row is mapped to the 3 values.
```scala
// Computes the row stats
// TODO: add example
val matrixRowStats = matrix.rowSizeAveStdev
def rowColValSymbols : List[Symbol] = List(rowSym, colSym, valSym)
```

### ColVector operations

Column vector operations are similar to the row vector ones, where all function names are renamed from row to col and the return type is in general ColVector.



## Matrix operations

### matrix1 \* matrix2: Matrix

Computes the product of two matrices
```scala
// Computes the product of two matrices
val matrixProd = matrix1 * matrix2
```

### matrix / scalar(LiteralScalar): Matrix

Computes the element-wise division of a matrix by a scalar
```scala
// Computes the element-wise division of a matrix by 100
val matrixDiv = matrix / 100
```

### matrix1.elemWiseOp(matrix2){ *function* }

Computes the element-wise merging of two matrices
```scala
// Computes the element-wise division of a matrix by another
matrix1.elemWiseOp(matrix2)((x,y) => x/y)
```

### matrix1 + matrix2: Matrix

Computes the sum of two matrices
```scala
// Computes the sum of two matrices
matrixSum = matrix1 + matrix2
```


### matrix1 - matrix2: Matrix

Computes the difference between two matrices
```scala
// Computes the difference between two matrices
matrixDiff = matrix1 - matrix2
```

### matrix1.hProd(matrix2): Matrix

Computes the element-wise product of two matrices
```scala
// Computes the element-wise product of two matrices
matrixProd = matrix1.hProd( matrix2 )
```

### matrix1.zip( matrix2/row/column ): Matrix

Merges the elements of the two matrices creating a matrix that has as values the pair tuples. Similarly, when zipping a matrix with a row or a vector, it zips the values of the row/column across all of the rows/columns of the matrix. The resulting value type is a pair tuple that has defined a Monoid trait.
```scala
// Returns the matrix with elements of the type ( 0, elem2), ( elem1, 0) and (elem1, elem2)
// matrix = 0 1 1
//          1 2 0
// rowVct = 1 0 1
// matrixVctPairs = (0, 1) (1, 0) (1, 1)
//                  (1, 1) (2, 0) (0, 1)
matrixPairs = matrix1.zip( matrix2 )
matrixVctPairs = matrix.zip ( rowVct )
```

### matrix.nonZerosWith(Scalar)

Zips the scalar on the right with all non-zeros in this matrix

```scala
// Similar to zip, but combine the scalar on the right with all non-zeros in this matrix:
matrixProd = matrix1.nonZerosWith( 100 )
```

### matrix.trace: Scalar

Computes the trace of a matrix that is equal to the sum of the diagonal values of the matrix.
```scala
// Computes the trace of a matrix
trace = matrix.trace
```

### matrix.sum: Scalar

Computes the sum of the elements of a matrix
```scala
// Computes the sum of the elements of a matrix
sum = matrix.sum
```

### matrix.transpose: Matrix

Computes the transpose of the matrix
```scala
// Computes the transpose of the matrix
matrixTranspose = matrix.transpose
```

### matrix.diagonal: DiagonalMatrix

Returns the diagonal of the matrix
```scala
// Returns the diagonal of the matrix
matrixDiag = matrix.diagonal
```


## ConversionFrom functions

### PipeExtensions

### pipe.toMatrix(field ): Matrix

Constructs a matrix from a pipe.
```scala
// The matrix will contain all of the data from the pipe and will have the names of the row, column and value dimensions ‘row, ‘col and ‘val. Again, the row and column types do not have to be numeric.
val matrix = pipe.toMatrix(‘row,’col,’val)
```

### pipe.mapToMatrix(fields)( *mapping function* ): Matrix

Constructs a matrix from a pipe by applying first a mapping function
```scala
// The new pipe contains all of the triples data in matrix with squared values
val matrix = pipe.mapToMatrix(‘a, ‘b, ‘c){ (x, y, z) => (x, y, z * z) }
```

### pipe.flatMapToMatrix(fields)( *mapping function* ): Matrix

Constructs a matrix from a pipe by applying first a mapping function
```scala
// The new pipe contains all of the triples data in matrix with log-values
val matrix = pipe.flatMapToMatrix( ‘a, ‘b, ‘c){ (x, y, z) => (x, y, scala.math.log(z)) }
```

### Companion object methods

## ConversionTo functions

### matrix.pipe: RichPipe

Returns the pipe associated with the matrix. The pipe contains tuples of the three (row,column,value) elements that can be accessed either using the field names or by using the Matrix class variables matrix.rowSym, matrix.colSym, matrix.valSym.
```scala
// The new pipe contains all of the triples data in matrix
val newPipe = matrix.pipe
```

### matrix.pipeAs(toFields): RichPipe

Returns the matrix pipe with the fields renamed
```scala
// The new pipe contains all of the triples data in matrix renamed to ('user,'movie,'similarity)
val pipeFields = matrix.pipeAs( ‘user, ‘movie, ‘similarity)
```

### matrix.write(src, outFields): Matrix

Writes to a sink and returns the matrix data for further processing.
```scala
// Writes to a sink and returns the matrix data for further processing.
matrix.write( Tsv("output") )
```

## Other functions

### matrix.fields: List[Symbol]

Returns the list of the fields of the pipe associated with the matrix: matrix.rowSym, matrix.colSym, matrix.valSym.
```scala
// The new pipe contains all of the triples data in matrix
val pipeFields = matrix.fields
```

### matrix.withSizeHint: Matrix

Adds a SizeHint to the matrix. The size hint specifies the approximate value of the dimensions of the matrix and impacts the type of joins that. If set to true, the skewness flag triggers an additional split of the data over a set of random keys to evenly distribute the computation.
```scala
// The new matrix has a new SizeHint specifying that there are around 1000 rows, 4000 columns, 1000 non-zeroes and that there is a skew of the values over the indexes.
val newMatrix = matrix.withSizeHint( 1000, 4000, 1000, true )
```

### matrix.hasHint: SizeHint

Returns true if the matrix has a size hint.
```scala
// Check if the matrix has a sizeHint set.
if (!matrix.hasHint) matrix.withSizeHint(newHint)
```

## Next steps

### Looking for the introduction or the tutorials? Here are the links:
* [Introduction to Matrix library](https://github.com/twitter/scalding/wiki/Introduction-to-Matrix-Library)
* [Matrix tutorials](https://github.com/twitter/scalding/tree/master/tutorial)
