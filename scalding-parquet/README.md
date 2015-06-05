# Parquet support for Scalding

The implementation is ported from code used by Twitter internally written by Sam Ritchie, Ian O'Connell, Oscar Boykin, Tianshuo Deng
## Use com.twitter.scalding.parquet.thrift for reading apache Thrift (TBase) records
## Use com.twitter.scalding.parquet.scrooge for reading scrooge Thrift (ThriftStruct) records
  Located in the scalding-parquet-scrooge module
## Use com.twitter.scalding.parquet.tuple for reading Tuple records
## Use com.twitter.scalding.parquet.tuple.TypedParquet for reading or writing case classes:
Can use macro in com.twitter.scalding.parquet.tuple.macros.Macros to generate parquet read/write support. Here's an example:
```scala
  import com.twitter.scalding.parquet.tuple.macros.Macros._

  case class SampleClass(x: Int, y: String)

  class WriteToTypedParquetTupleJob(args: Args) extends Job(args) {
    val outputPath = args.required("output")
    val sink = TypedParquetSink[SampleClass](outputPath)

    TypedPipe.from(List(SampleClass(0, "foo"), SampleClass(1, "bar"))).write(sink)
  }

  class ReadWithFilterPredicateJob(args: Args) extends Job(args) {
    val fp: FilterPredicate = FilterApi.eq(binaryColumn("y"), Binary.fromString("foo"))

    val inputPath = args.required("input")
    val outputPath = args.required("output")

    val input = TypedParquet[SampleClass](inputPath, fp)

    TypedPipe.from(input).map(_.x).write(TypedTsv[Int](outputPath))
  }
```