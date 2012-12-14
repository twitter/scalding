
  implicit def rowMatrixProduct[Common, RowR, ValT](implicit ring: Ring[ValT]) :
    MatrixProduct[RowVector[Common, ValT], Matrix[RowR, Common, ValT], RowVector[Common, ValT]] =
    new MatrixProduct[RowVector[Common, ValT], Matrix[RowR, Common, ValT], RowVector[Common, ValT]] {
      def apply(left: RowVector[Common, ValT], right: Matrix[RowR, Common, ValT]) = {
        (left.toMatrix(true) * right).toRow
      }
    }

  implicit def matrixColProduct[Common, RowR, ValT](implicit ring: Ring[ValT]) :
    MatrixProduct[Matrix[Common, ColR, ValT], ColVector[Common, ValT], ColVector[Common, ValT]] =
    new MatrixProduct[Matrix[Common, ColR, ValT], ColVector[Common, ValT], ColVector[Common, ValT]] {
      def apply(left: Matrix[Common, ColR, ValT], right: ColVector[Common, ValT]) = {
        (left * right.toMatrix(true)).toCol 
      }
    }
