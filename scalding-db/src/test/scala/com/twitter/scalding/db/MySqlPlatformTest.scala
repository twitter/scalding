package com.twitter.scalding.db

import com.twitter.scalding._
import com.twitter.scalding.platform._
import com.twitter.scalding.db.macros._
import com.twitter.scalding.db.driver._

import org.apache.hadoop.mapred.JobConf
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }

import java.sql._

case class PlatformTestRecord(
  id: Int,
  @size(64) user_name: String)

case class MySqlPlatformTestSource() extends TypedJDBCSource[PlatformTestRecord](AvailableDatabases(Map(
  Database("local_h2") -> ConnectionSpec(
    ConnectUrl("jdbc:h2:~/test;MODE=MySQL"), UserName(""), Password(""), Adapter("h2"), StringEncoding("UTF8"))))) {
  override val database = Database("local_h2")
  override val tableName = TableName("mysql_platform_test")
}

class SimpleWriteJob(args: Args) extends Job(args) {
  @transient implicit val dbs = AvailableDatabases()

  TypedPipe.from((1 to 100).map { id => PlatformTestRecord(id, s"user_$id") })
    .write(MySqlPlatformTestSource())
}

class SimpleReadJob(args: Args, policy: QueryPolicy = QueryOnSubmitter) extends Job(args) {
  @transient implicit val dbs = AvailableDatabases()

  TypedPipe.from(new MySqlPlatformTestSource() { override val queryPolicy = policy })
    .map { r => (r.id, r.user_name) }
    .write(TypedTsv[(Int, String)](args("output")))
}

class SimpleReadIteratorJob(args: Args, policy: QueryPolicy = QueryOnSubmitter) extends Job(args) {
  @transient implicit val dbs = AvailableDatabases()

  TypedPipe.from(new MySqlPlatformTestSource() { override val queryPolicy = policy }.toIterator.toList)
    .map { r => (r.id, r.user_name) }
    .write(TypedTsv[(Int, String)](args("output")))
}

class SimpleReplaceOnInsertJob(args: Args) extends Job(args) {
  @transient implicit val dbs = AvailableDatabases()

  TypedPipe.from((1 to 100).map { id => PlatformTestRecord(id, s"user_${id}_replaced") })
    .write(new MySqlPlatformTestSource() { override val replaceOnInsert = true })
}

class MySqlPlatformTest extends WordSpec with Matchers with BeforeAndAfterAll {

  private[this] val cluster = LocalCluster()

  private[this] val jdbcSource = MySqlPlatformTestSource()

  private[this] def h2Connection = {
    Class.forName(H2DbDriver().driver.toStr)
    val connConf = jdbcSource.currentConfig
    DriverManager.getConnection(connConf.connectUrl.toStr)
  }

  override def beforeAll() {
    System.setProperty("cascading.update.skip", "true")

    val conn = h2Connection
    conn.createStatement().execute("DROP ALL OBJECTS")
    conn.close()

    cluster.synchronized {
      org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN)
      org.apache.log4j.Logger.getLogger("org.apache.hadoop.metrics2.util").setLevel(org.apache.log4j.Level.ERROR)
      org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.FATAL)

      cluster.initialize()
    }
    super.beforeAll()
  }

  private[this] def verifyWrites(check: (ResultSet, Int) => Unit) = {
    val conn = h2Connection
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(s"SELECT * FROM ${jdbcSource.tableName.toStr}")
      var count = 0
      while (rs.next()) {
        count = count + 1
        check(rs, count)
      }
      assert(count == 100)
    } finally {
      stmt.close()
      conn.close()
    }
  }

  private[this] def verifyReads(remotePath: String, linemaker: Int => String) = {
    val hmode = cluster.mode.asInstanceOf[HadoopMode]
    val conf = hmode.jobConf
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val localPath = java.nio.file.Files.createTempDirectory("hadoopMysqlTest").toString
    fs.copyToLocalFile(new org.apache.hadoop.fs.Path(remotePath), new org.apache.hadoop.fs.Path(localPath))

    val files = new java.io.File(s"$localPath/read_output").listFiles
      .filter(_.getName.startsWith("part-"))

    var readCount = 0
    files.foreach { f =>
      val reader = scala.io.Source.fromFile(f)
      reader.getLines().foreach { line =>
        readCount = readCount + 1
        assert(linemaker(readCount) == line)
      }
      reader.close()
    }
    assert(readCount == 100)
  }

  "JDBCSource for MySQL" should {

    "Write and read" in {
      val args = Mode.putMode(cluster.mode, new Args(Map.empty))
      new SimpleWriteJob(args).run
      verifyWrites { (rs: ResultSet, count: Int) =>
        assert(count == rs.getInt("id"))
        assert(s"user_$count" == rs.getString("user_name"))
      }

      val remotePath = "read_output"
      val readArgs = Mode.putMode(cluster.mode, new Args(Map("output" -> List(remotePath))))
      def linemaker(count: Int) = s"${count}\tuser_${count}" // TypedTsv output

      new SimpleReadJob(readArgs).run
      verifyReads(remotePath, linemaker)

      new SimpleReadIteratorJob(readArgs).run
      verifyReads(remotePath, linemaker)

      new SimpleReadJob(readArgs, QueryOnMappers).run
      verifyReads(remotePath, linemaker)

      new SimpleReadIteratorJob(readArgs, QueryOnMappers).run
      verifyReads(remotePath, linemaker)

      // set primary key for replaceOnInsert test
      val conn2 = h2Connection
      val alterStmt = conn2.createStatement()
      try {
        alterStmt.execute(s"ALTER TABLE ${jdbcSource.tableName.toStr} ADD PRIMARY KEY (id)")
      } finally {
        alterStmt.close()
        conn2.close()
      }

      new SimpleReplaceOnInsertJob(args).run
      verifyWrites { (rs: ResultSet, count: Int) =>
        assert(count == rs.getInt("id"))
        assert(s"user_${count}_replaced" == rs.getString("user_name"))
      }
    }
  }

  override def afterAll() {
    try super.afterAll()
    finally {
      cluster.synchronized {
        cluster.shutdown()
      }
      val conn = h2Connection
      conn.createStatement().execute("DROP ALL OBJECTS")
      conn.close()
    }
  }
}
