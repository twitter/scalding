package com.twitter.scalding.db

import com.twitter.scalding._

import cascading.tap.Tap
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import cascading.tap.SinkMode

import scala.util.{ Try, Success, Failure }

object VerticaSink {
  def apply[T: DBTypeDescriptor: VerticaRowSerializer](database: Database,
    tableName: TableName,
    schema: SchemaName)(implicit dbsInEnv: AvailableDatabases): VerticaSink[T] =
    VerticaSink[T](dbsInEnv(database), tableName, schema, None, None)

  def apply[T: DBTypeDescriptor: VerticaRowSerializer](database: Database,
    tableName: TableName,
    schema: SchemaName,
    preloadQuery: Option[SqlQuery],
    postloadQuery: Option[SqlQuery])(implicit dbsInEnv: AvailableDatabases): VerticaSink[T] =
    VerticaSink[T](dbsInEnv(database), tableName, schema, preloadQuery, postloadQuery)

  // Used in testing
  def nullCompletionHandler = new JdbcSinkCompletionHandler(null) {
    override def commitResource(conf: JobConf, path: String): Boolean =
      true
  }
}

case class VerticaSink[T: DBTypeDescriptor: VerticaRowSerializer](
  connectionConfig: ConnectionConfig,
  tableName: TableName,
  schema: SchemaName,
  override val preloadQuery: Option[SqlQuery] = None,
  override val postloadQuery: Option[SqlQuery] = None,
  skipCompletionHandler: Boolean = false,
  optionalPath: Option[String] = None) extends Source with TypedSink[T] with JDBCLoadOptions {

  private val jdbcTypeInfo = implicitly[DBTypeDescriptor[T]]

  val columns = jdbcTypeInfo.columnDefn.columns

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

  @transient lazy val verticaWriter = new VerticaJdbcWriter(
    tableName,
    schema,
    connectionConfig,
    columns,
    AdditionalQueries(preloadQuery, postloadQuery))

  @transient lazy val completionHandler = new JdbcSinkCompletionHandler(verticaWriter)

  @transient private[this] val verticaHeader = new VerticaHeader[T] {
    override val bytes = NativeVertica.headerFrom(columns)
  }

  /** The scheme to use if the source is on hdfs. */
  def hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] =
    HadoopSchemeInstance(
      new VerticaNativeScheme[T](
        new VerticaRowWrapperFactory[T](
          implicitly[VerticaRowSerializer[T]]),
        verticaHeader).asInstanceOf[cascading.scheme.Scheme[_, _, _, _, _]])

  val sinkMode: SinkMode = SinkMode.REPLACE

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      // TODO support strict in Local
      case Local(_) => sys.error("Local mode not supported for the VerticaSink")
      case Hdfs(_, conf) => readOrWrite match {
        case Read => sys.error("Read mode not supported VerticaSink")
        case Write =>
          val handler = if (skipCompletionHandler) VerticaSink.nullCompletionHandler else completionHandler
          optionalPath.map { p =>
            CastHfsTap(new VerticaSinkTap(hdfsScheme, new JobConf(conf), sinkMode, handler, p))
          }.getOrElse(CastHfsTap(new VerticaSinkTap(hdfsScheme, new JobConf(conf), sinkMode, handler)))
      }
      case _ => {
        val tryTtp = Try(TestTapFactory(this, hdfsScheme, sinkMode)).map {
          // these java types are invariant, so we cast here
          _.createTap(readOrWrite)
            .asInstanceOf[Tap[Any, Any, Any]]
        }

        tryTtp match {
          case Success(s) => s
          case Failure(e) => throw new java.lang.IllegalArgumentException(s"Failed to create tap for: $toString, with error: ${e.getMessage}", e)
        }
      }
    }
  }

}
