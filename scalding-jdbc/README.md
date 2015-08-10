### Deprecated

`scalding-jdbc` has been deprecated in favor of `scalding-db`.

In order to upgrade to the new module:

##### A. Update dependencies and imports
1. Change your dependency from `scalding-jdbc` to `scalding-db`
2. Change the imports

```scala
import com.twitter.scalding.jdbc.JDBCSource
```
should now be:
```scala
import com.twitter.scalding.db.JDBCSource
```

##### B. Source modifications

If you have a source defined like:
```scala
class ExampleMysqlJdbcSource extends JDBCSource with MysqlDriver {
  override val tableName = TableName("test")
  override val columns: Iterable[ColumnDefinition] = Iterable( ... )
  override val currentConfig = ConnectionSpec(
    ConnectUrl("url"),
    UserName("username"),
    Password("password"))
}
```
modify it as:
```scala
class ExampleMysqlJdbcSource extends JDBCSource /* no more driver trait import*/ {
  override val tableName = TableName("test")
  override val columns: Iterable[ColumnDefinition] = Iterable( ... )
  /* db driver/adapter now goes in ConnectionSpec */
  override val currentConfig = ConnectionSpec(
    ConnectUrl("url"),
    UserName("username"),
    Password("password"),
    Adapter("mysql"))
}
```

NOTE: For new sources, it is recommended to use the new `TypedJDBCSource` from `scalding-db`. It is fully Typed API compatible.
