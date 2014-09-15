# Parquet support for Scalding

## parquet.thrift and parquet.tuple
The implementation is ported from code used by Twitter internally written by Sam Ritchie, Ian O'Connell, Oscar Boykin, Tianshuo Deng

Use com.twitter.scalding.parquet.thrift for reading Thrift records

### parquet.tuple
Use com.twitter.parquet.tuple for reading Tuple records.
```scala
import com.twitter.scalding._
import com.twitter.scalding.parquet.tuple._

new FixedPathParquetTuple(("field1", "field2"), "input-path")
  .read
  .map(('field1, 'field2) -> 'field3) { fields: (String, Int) =>
    val (field1, field2) = fields
    
    field1 + field2
  }
```

## parquet.avro
The implementation is ported from code used by Tapad internally written by Dag Liodden, Jeffrey Olchovy and Oleksii Iepishkin.

Use com.twitter.scalding.parquet.avro to read and write Avro records. Projection is used to read only a subset of fields. You can project nested fields, unions and arrays.
```scala
import com.twitter.scalding.parquet.avro._

ParquetAvroSource.project[Signal]("input-path", Projection[Signal]("field1", "field2.field2_1"))
  .read
  
  //do some transformations on this pipe
  //...
  
  //mapTo avro field that we write to parquet
  .mapTo(('field1, 'field2) -> 'field3) { fields: (String, Int) =>
    val (field1, field2) = fields
    
    OtherSignal.newBuilder()
      .setField1(field1)
      .setField2(field2)
      .build()
  }
  
  .write(ParquetAvroSource[OtherSignal]("output-path"))
```
