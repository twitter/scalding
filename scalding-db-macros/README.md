## Scalding JDBC Macros

Provides macros to interop between Scala case classes and relational database / SQL column definitions.

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

## Nested case class

Nested case classes can be used as a workaround for the 22-size limitation. It can also be used for logically grouping the table columns. Nested case classes are flattened in left to right order. For example:
```scala
case class Person(id: Long, name: String, location: Location)
case class Location(geo: GeoCode, doorNum: Int, street: String, city: String)
case class GeoCode(lat: Long, lng: Long)
```
is flattened to a table schema with columns `id`, `name`, `lat`, `lng`, `doorNum`, `street`, `city`.
