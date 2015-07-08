package object reducer_estimation {
  def median(xs: Seq[Long]): Option[Long] = xs.sorted.lift(xs.length / 2)
  def mean(xs: Seq[Long]): Option[Long] = if (xs.isEmpty) None else Some(xs.sum / xs.length)
}
