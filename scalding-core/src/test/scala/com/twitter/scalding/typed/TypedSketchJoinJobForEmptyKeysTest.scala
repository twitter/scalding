class TypedSketchJoinJobForEmptyKeys(args: Args) extends Job(args) {
  // Deal with when a key appears in left but not right
  val leftTypedPipe = TypedPipe.from(List((1, 1111)))
  val rightTypedPipe = TypedPipe.from(List((3, 3333), (4, 4444)))

  implicit def serialize(k: Int) = k.toString.getBytes
  leftTypedPipe
    .sketch(1)
    .leftJoin(rightTypedPipe)
    .map{
      case (a, (b, c)) =>
        (a, b, c.getOrElse(-1))
    }
    .write(TypedTsv(Int, Int, Int))

  leftTypedPipe
    .group
    .leftJoin(rightTypedPipe.group)
    .map{
      case (a, (b, c)) =>
        (a, b, c.getOrElse(-1))
    }
    .write(TypedTsv(Int, Int, Int))
}
