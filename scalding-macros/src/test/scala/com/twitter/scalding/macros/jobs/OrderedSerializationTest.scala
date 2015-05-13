package com.twitter.scalding.macros.jobs

import java.util.TimeZone

import com.twitter.scalding._
import com.twitter.scalding.platform.{HadoopPlatformJobTest, HadoopPlatformTest}
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.typed.TypedPipe
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks

import scala.language.experimental.macros
import scala.math.{Ordering, Ordered}
import com.twitter.scalding.RichDate

/**
 * @author Mansur Ashraf.
 */
class OrderedSerializationTest extends FunSuite with PropertyChecks with HadoopPlatformTest {

  test("Test Fork/Join") {
    import AuroraJobRecord._

    forAll(maxSize(1)) { (in1: List[Sample], in2: List[AuroraJobRecord]) =>

      val fn = (arg: Args) => new ComplexJob(in1, in2, arg)

      HadoopPlatformJobTest(fn, cluster)
        .arg("output1", "output1")
        .arg("output2", "output2")
        .sink[(ContainerStat, EnhancedAuroraJobRecord)](TypedTsv[(ContainerStat, EnhancedAuroraJobRecord)]("output2")) {
        actual =>
          fail("should not reach this block")
      }.sink[(String, AuroraJobKey)](TypedTsv[(String, AuroraJobKey)]("output1")) { x => () }
        .run
    }
  }


  test("Test serialization") {
    import RecordContainer._

    forAll(maxSize(10)) { in: List[RecordContainer] =>
      val expected = in.groupBy(r => (r.c, r.record))
        .mapValues(i => i.map(_.c).sum).toList

      val fn = (arg: Args) => new TestJob1(in, arg)

      JobTest(fn)
        .arg("output", "output")
        .sink[((Int, Record), Int)](TypedTsv[((Int, Record), Int)]("output")) {
        actual =>
          assert(expected.sortBy { case ((_, x), _) => x } == actual.toList.sortBy { case ((_, x), _) => x })
      }
        .run
    }
  }
}

case class Record(a: String, b: String) extends Ordered[Record] {
  override def compare(that: Record): Int = a.compareTo(that.a) match {
    case 0 => b.compareTo(that.b)
    case x => x
  }
}

case class RecordContainer(c: Int, record: Record)

object RecordContainer {
  implicit val arbRecord: Arbitrary[Record] = Arbitrary {
    for {
      a <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      b <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
    } yield Record(a, b)
  }

  implicit val arbRecordContainer: Arbitrary[RecordContainer] = Arbitrary {
    for {
      d <- Gen.choose(0, Int.MaxValue)
      r <- Arbitrary.arbitrary[Record]
    } yield RecordContainer(d, r)
  }

  implicit val arb: Arbitrary[List[RecordContainer]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[RecordContainer]).filter(_.nonEmpty)
  }
}

trait RequiredBinaryComparators extends Job {

  implicit def primitiveOrderedBufferSupplier[T]: OrderedSerialization[T] = macro com.twitter.scalding.macros.impl.OrderedSerializationProviderImpl[T]

  override def config =
    super.config + ("scalding.require.orderedserialization" -> "true")
}

class TestJob1(input: List[RecordContainer], args: Args) extends Job(args) with RequiredBinaryComparators {

  assert(implicitly[Ordering[(Int, Record)]].isInstanceOf[Ordering[OrderedSerialization[_]]], "wrong ordering!")

  val pipe1 = TypedPipe.from(input)
    .groupBy(r => (r.c, r.record))

  val pipe2 = TypedPipe.from(input)
    .groupBy(r => (r.c, r.record))

  pipe1.join(pipe2)
    .mapValues { case (left, _) => left.c }
    .sum
    .write(TypedTsv[((Int, Record), Int)](args("output")))
}

class ComplexJob(input1: List[Sample], input2: List[AuroraJobRecord], args: Args) extends Job(args) with RequiredBinaryComparators {

  def getAuroraDataForDay(): TypedPipe[EnhancedAuroraJobRecord] = {

    TypedPipe
      .from(input2)
      .map(EnhancedAuroraJobRecord.apply)
      .groupBy(r => (r.day, r.key))(primitiveOrderedBufferSupplier[Tuple2[RichDate, AuroraJobKey]])
      .reduce(EnhancedAuroraJobRecord.reduce)
      .values
    //  .forceToDisk
  }

  /**
   * Get container stats for Aurora jobs over a 24h period.
   */
  def getContainerStatsForDay: TypedPipe[ContainerStat] = {

    TypedPipe
      .from(input1)
      .filter(s => s.serviceName == "mesos.container")
      // This ensures we only look at container stats for Aurora tasks.
      .filter(s => s.source.startsWith("sd."))
      // Mapping the Source thrift to our own case class greatly reduces the MR job runtime
      .map(s => ContainerStat(s))
      // There can be multiple ContainerStat instances for a job and and time.
      // They need to be reduced to a single sample.
      .groupBy(s => (s.source, s.timestamp, s.zone))(primitiveOrderedBufferSupplier[Tuple3[String, Long, String]])
      .reduce(ContainerStat.reduce)
      .values
    //    .forceToDisk
  }

  /**
   * Join container stats with Aurora data and discard jobs that have no stats and vice versa
   */

  def joinContainerStatWithAuroraData(auroraData: TypedPipe[EnhancedAuroraJobRecord],
                                      containerStats: TypedPipe[ContainerStat]) = {

    // Joins container stats with aurora metadata
    implicit val tz = TimeZone.getDefault
    val stats = containerStats.groupBy(AuroraSampleGroupingKey.apply)
    val metadata = auroraData.groupBy(AuroraSampleGroupingKey.apply)

    stats
      .keys
      .map(s => (s.day.toString(), s.key))
      .write(TypedTsv[(String, AuroraJobKey)](args("output1")))



    stats.join(metadata)
      .values
      .map(t => (t._1, t._2))
  }

  joinContainerStatWithAuroraData(
    getAuroraDataForDay(),
    getContainerStatsForDay)
    .write(TypedTsv[(ContainerStat, EnhancedAuroraJobRecord)](args("output2")))
}


object AuroraJobRecord {

  implicit val sample = Arbitrary {
    for {
      a <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      b <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      c <- Arbitrary.arbitrary[Long]
      d <- Arbitrary.arbitrary[Long]
      e <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      f <- Arbitrary.arbitrary[Option[String]]
    } yield Sample(a, b, c, d, e, f)
  }

  implicit val sampleList: Arbitrary[List[Sample]] = Arbitrary {
    Gen.nonEmptyListOf(Arbitrary.arbitrary[Sample])
  }

  implicit val auroraRecord = Arbitrary {
    for {
      a <- Arbitrary.arbitrary[Long]
      b <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      c <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      d <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      e <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      f <- Arbitrary.arbitrary[Boolean]
      g <- Arbitrary.arbitrary[Boolean]
      h <- Arbitrary.arbitrary[Boolean]
    } yield AuroraJobRecord(a, b, c, d, e, f, g, h)
  }

  implicit val jobList: Arbitrary[List[AuroraJobRecord]] = Arbitrary {
    Gen.nonEmptyListOf(Arbitrary.arbitrary[AuroraJobRecord])
  }
}

case class AuroraJobRecord(timestamp: Long,
                           zone: String,
                           role: String,
                           env: String,
                           name: String,
                           isProduction: Boolean,
                           isCron: Boolean,
                           isDedicated: Boolean)

case class AuroraJobKey(zone: String,
                        role: String,
                        env: String,
                        name: String) extends Ordered[AuroraJobKey] {
  override def toString = "%s/%s/%s/%s".format(zone, role, env, name)

  override def compare(that: AuroraJobKey): Int = this.toString compare that.toString
}

object EnhancedAuroraJobRecord {
  def apply(r: AuroraJobRecord): EnhancedAuroraJobRecord = {
    EnhancedAuroraJobRecord(
      RichDate(r.timestamp),
      AuroraJobKey(r.zone, r.role, r.env, r.name),
      r.isProduction,
      r.isCron,
      r.isDedicated)
  }

  def reduce(a: EnhancedAuroraJobRecord, b: EnhancedAuroraJobRecord): EnhancedAuroraJobRecord = {
    require(a.key == b.key)
    require(a.day == b.day)
    // Prefer the production one
    val state = ((a.isDedicated, b.isDedicated), (a.isProduction, b.isProduction), (a.isCron, b.isCron))

    // If a job was dedicated, production or cron at any time during the day, then treat it as such
    // for the entirety of the day.
    state match {
      case ((true, false), _, _) => a
      case ((false, true), _, _) => b
      case (_, (true, false), _) => a
      case (_, (false, true), _) => b
      case (_, _, (true, false)) => a
      case (_, _, (false, true)) => b
      case _ => a
    }
  }
}

case class EnhancedAuroraJobRecord(day: RichDate,
                                   key: AuroraJobKey,
                                   isProduction: Boolean,
                                   isCron: Boolean,
                                   isDedicated: Boolean) extends Ordered[EnhancedAuroraJobRecord] {
  // Alphabetical ordering
  override def compare(that: EnhancedAuroraJobRecord): Int =
    Ordering[String].compare(this.key.toString, that.key.toString)
}

case class ContainerStat(timestamp: Long,
                         source: String,
                         zone: String,
                         cpuTimeSecs: Option[Long],
                         cpuReservation: Double,
                         memUsage: String,
                         memReservation: String,
                         diskUsage: String,
                         diskReservation: String) {


}

object ContainerStat {
  def apply(sample: Sample): ContainerStat = {

    val ts = sample.timestampMillis
    val source = sample.source
    val zone = sample.zone.get
    // Depending on the type of data we treat missing data differently.
    // Since cpu time is a cumulative metric that needs to be turned into deltas, it is better to
    // use Option so the calling code can make choices on missing samples.
    // Other metrics can be treated as 0.



    ContainerStat(ts, source, zone, None, 0.0, "", "", "", "")
  }

  def reduce(a: ContainerStat, b: ContainerStat): ContainerStat = {
    a
  }


}


case class Sample(
                   serviceName: String,
                   source: String,
                   timestampMillis: Long,
                   granularityMillis: Long,
                   metrics: String,
                   zone: Option[String])

object AuroraSampleGroupingKey {
  def apply(r: EnhancedAuroraJobRecord): AuroraSampleGroupingKey = {
    AuroraSampleGroupingKey(r.day, r.key)
  }

  def apply(s: ContainerStat)(implicit tz: TimeZone): AuroraSampleGroupingKey = {
    val day = RichDate(s.timestamp)
    val zone = s.zone

    val keyElements = s.source.split('.').drop(1)
    val role = keyElements(0)
    val env = keyElements(1)
    // As shown in AURORA-3875 job names might have a '.' in them.
    val name = keyElements.drop(2).mkString(".")
    val key = AuroraJobKey(zone, role, env, name)

    AuroraSampleGroupingKey(day, key)
  }
}

case class AuroraSampleGroupingKey(day: RichDate, key: AuroraJobKey) {
}




