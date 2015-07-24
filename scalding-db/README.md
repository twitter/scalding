## scalding-db

`scalding-db` module provides Scalding sources for connecting to relational databases. A source can be created using two simple steps:

Step 1 -- Defining a case class that represents your relational table schema:

```scala
case class ExampleDBRecord(
  card_id: Long,
  tweet_id: Long,
  created_at: Option[java.util.Date],
  deleted: Boolean = false)
```

Step 2 -- Defining a `TypedJDBCSource` based on the above case class:

```scala
case class ExampleDBRecordsTable(implicit dbsInEnv : AvailableDatabases)
    extends TypedJDBCSource[ExampleDBRecord](dbsInEnv) {
  override val tableName = TableName("example_table")
  override val database = Database("example_schema") 
  override val filterCondition = Some("deleted=0")
}
```

Under the hood, the supplied case class is automatically mapped to the underlying DB schema using Scala macros.


### Supported Mappings

Scala type  | SQL type
------------- | -------------
`Int` | `INTEGER`
`Long` | `BIGINT`
`Short` | `SMALLINT`
`Double` | `DOUBLE`
`@varchar @size(20) String `| `VARCHAR(20)`
`@text String` | `TEXT`
`java.util.Date` | `DATETIME`
`@date java.util.Date` | `DATE`
`Boolean` | `BOOLEAN`
          |  <sub><sup>(`BOOLEAN` is used if creating a new table at write time, but `BOOL` and `TINYINT` are also supported for reading existing columns)</sup></sub>

* Annotations are used for String types to clearly distinguish between TEXT and VARCHAR column types
* Scala `Option`s can be used to denote columns that are `NULLABLE` in the DB
* `java.lang.*` types are not supported. For e.g. `Integer` (`java.lang.Integer`) does not work

#### Working with nested case classes

Nested case classes can be used as a workaround for the 22-size limitation. They can also be used for logically grouping the table columns. Nested case classes are flattened in left to right order. For example:
```scala
case class Person(id: Long, name: String, location: Location)
case class Location(geo: GeoCode, doorNum: Int, street: String, city: String)
case class GeoCode(lat: Long, lng: Long)
```
is flattened to a table schema with columns `id`, `name`, `lat`, `lng`, `doorNum`, `street`, `city`.

#### Working with NULLABLE column types
`Option`s can be used to denote columns that are marked as NULLABLE in the DB. Those will automatically be converted to None for null values.

Currently, the following databases are supported:
* MySQL, both reads and writes
* Vertica, writes only


## MySQL usage

### Reads

For reads, a streaming copy from MySQL to HDFS is performed via the submitter prior to kicking off the mapreduce tasks. This HDFS snapshot is used for reads during the lifetime of the Scalding job.

There is only one JDBC connection that is opened on the submitter for this snapshotting. No JDBC connections are opened from mapreduce tasks where it can be hard to control connections from a variable number of mappers or reducers.

This is intended to be a more effective solution for the most common use-cases around reading from MySQL in Scalding that involve reading small to moderate amount of data.

The number of HDFS part files can be controlled by specifying the number of records per file:
```scala
override def maxRecordsPerFile = Some(100000)
```

Before the reads are initiated, there is a validation step to check if the case class matches with the actual DB schema (as provided by java.sql.ResultSetMetadata). This helps fail the job early in case there is a schema mismatch.

If you want to disable this:
```scala
override def dbSchemaValidation = false
```

For very large datasets, there is an option to read splits directly on mappers. It can be used in conjunction with maxConcurrentReads to tune the number of mappers. This option should be used only if snapshot via submitter is too slow for your use case.

```scala
override def queryPolicy = QueryOnMappers
override def maxConcurrentReads = 10 // tune the number of mappers
```

NOTE: When using `QueryOnMappers`, it is recommended that `.forceToDisk` be called in the related `TypedPipe` as early as possible. This ensure MySQL data is checkpointed in HDFS for reuse in downstream steps, thereby, avoided excessive number of JDBC queries.

### Writes

All the output data is staged in a HDFS directory and is loaded to MySQL after the related MR step completes. This means that the load query is run via the submitter node, and you do not need a groupAll to ensure that only one node writes to the database.

#### Batch size for writes

`batchSize` defines the number of rows in each update sent to MySQL during writes. For tables with large row sizes, it can be configured to a smaller number to avoid OOMing the submitter node:

```scala
override val batchSize = 200 // default is 1000
```

#### Duplicate key updates

Turning on replaceOnInsert option uses MySQL's `ON DUPLICATE KEY UPDATE` to update existing records. Note that this requires that:

1. `UNIQUE KEY` constraints are already set correctly in your table schema

2. The columns in the constraints are part of your case class definition

```scala
override val replaceOnInsert = true
```

## Vertica usage

### Writes

There is a special `VerticaSink` for exporting data to Vertica from your Scalding job.

```scala 
yourTypedPipe
  .write(VerticaSink[ExampleDBRecord](Database("db_name"),
    TableName("table_name"),
    SchemaName("schema_name"))
```

Writes are implemented using Vertica's parallel load feature. This means that the different part files in your HDFS source are loaded into Vertica in parallel, but under a single transaction.

This is significantly more efficient when loading a large amount of data and avoids bottlenecking Vertica from doing a single, sequential copy of the entire dataset.

For more, see: http://www.vertica.com/2012/07/05/teaching-the-elephant-new-tricks/

We use Vertica's native file format to stage the LOAD output on HDFS: http://my.vertica.com/docs/5.0/HTML/Master/13562.htm

NOTE: Reading from Vertica is not supported.

## Scalding DB macros in detail

Macros are used for interop between Scala case classes and relational database / SQL column definitions.

For a case class T, the macro-generated `ColumnDefinitionProvider[T]` provides:

1. `ColumnDefinition`s for the corresponding DB table columns

2. `ResultSetExtractor[T]` for extracting records from `java.sql.ResultSet` into objects of type `T`

Also provided are `TupleConverter`, `TupleSetter` and `cascading.tuple.Fields` for use with Cascading.
`DBTypeDescriptor[T]` is the top-level class that contains all of the above.

### Illustration

(in the REPL)

Necessary imports:

    scalding> import com.twitter.scalding.db_
    scalding> import com.twitter.scalding.db.macros._

Case class representing your DB schema:

    scalding> case class ExampleDBRecord(
       |   card_id: Long,
       |   tweet_id: Long,
       |   created_at: Option[java.util.Date],
       |   deleted: Boolean = false)
    defined class ExampleDBRecord

Get the macro-generated converters:

    scalding> val dbTypeInfo = implicitly[DBTypeDescriptor[ExampleDBRecord]]
    dbTypeInfo: com.twitter.scalding.db.DBTypeDescriptor[ExampleDBRecord] = $anon$6@7b07168

    scalding> val columnDefn = dbTypeInfo.columnDefn
    columnDefn: com.twitter.scalding.db.ColumnDefinitionProvider[ExampleDBRecord] = $anon$6$$anon$2@53328a4f

Macro-generated SQL column definitions:

    scalding> columnDefn.columns
    res0: Iterable[com.twitter.scalding.db.ColumnDefinition] =
    List(
      ColumnDefinition(BIGINT,ColumnName(card_id),NotNullable,None,None),
      ColumnDefinition(BIGINT,ColumnName(tweet_id),NotNullable,None,None),
      ColumnDefinition(DATETIME,ColumnName(created_at),Nullable,None,None),
      ColumnDefinition(BOOLEAN,ColumnName(deleted),NotNullable,None,Some(false))
    )

Macro-generated Cascading fields:

    scalding> dbTypeInfo.fields
    res1: cascading.tuple.Fields = 'card_id', 'tweet_id', 'created_at', 'deleted | long, long, Date, boolean
