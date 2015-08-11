package object reducer_estimation {
  def median(xs: Seq[Double]): Option[Double] = xs.sorted.lift(xs.length / 2)
  def mean(xs: Seq[Double]): Option[Double] = if (xs.isEmpty) None else Some(xs.sum / xs.length)
}
