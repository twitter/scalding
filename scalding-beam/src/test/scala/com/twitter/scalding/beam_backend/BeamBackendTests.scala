package com.twitter.scalding.beam_backend

import com.twitter.algebird.{AveragedValue, Semigroup}
import com.twitter.scalding.beam_backend.BeamOp.{CoGroupedOp, FromIterable, HashJoinOp, MergedBeamOp}
import com.twitter.scalding.{Config, TextLine, TypedPipe}
import java.io.File
import java.nio.file.Paths
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.io.Source

class BeamBackendTests extends FunSuite with BeforeAndAfter {

  private var pipelineOptions: PipelineOptions = _
  private var testPath: String = _

  def beamMatchesSeq[A](t: TypedPipe[A], expectedResult: Seq[A], config: Config = Config.empty) = {
    val bmode = BeamMode.default(pipelineOptions)
    val outRoute = tmpPath("out")
    t.map(_.toString).writeExecution(TextLine(outRoute)).waitFor(config, bmode).get
    val result = getContents(testPath, outRoute).sorted
    assert(result == expectedResult.map(_.toString).sorted)
  }

  def beamPlan[A](t: TypedPipe[A], config: Config = Config.empty): (Pipeline, BeamOp[A]) = {
    val bmode = BeamMode.default(pipelineOptions)
    val planner = BeamPlanner.plan(config, bmode.sources)
    val pipeline = Pipeline.create(bmode.pipelineOptions)
    (pipeline, planner(t))
  }

  before {
    testPath = Paths.get(System.getProperty("java.io.tmpdir"), "scalding", "beam_backend").toString
    pipelineOptions = PipelineOptionsFactory.create()
  }

  after {
    removeDir(testPath)
  }

  def tmpPath(suffix: String): String =
    Paths.get(testPath, suffix).toString

  test("BeamOp caching: FromIterable") {
    val a = TypedPipe.from(0 to 5)

    val (pipeline, op) = beamPlan(a)

    assert(op.isInstanceOf[FromIterable[Int]])
    assert(op.run(pipeline) == op.run(pipeline))
  }

  test("BeamOp caching: CoGroupedOp") {
    val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
    val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))

    val (pipeline, op) = beamPlan(leftPipe.join(rightPipe))

    assert(op.isInstanceOf[CoGroupedOp[Int, Int]])
    assert(op.run(pipeline) == op.run(pipeline))
  }

  test("BeamOp caching: HashJoinOp") {
    val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
    val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))

    val (pipeline, op) = beamPlan(leftPipe.hashJoin(rightPipe))

    assert(op.isInstanceOf[HashJoinOp[Int, Int, Int, Int]])
    assert(op.run(pipeline) == op.run(pipeline))
  }

  test("BeamOp caching: MergedBeamOp") {
    val a = TypedPipe.from(0 to 5)
    val b = TypedPipe.from(6 to 10)

    val (pipeline, op) = beamPlan(a ++ b)

    assert(op.isInstanceOf[MergedBeamOp[Int]])
    assert(op.run(pipeline) == op.run(pipeline))
  }

  test("BeamOp caching: Source") {
    val source = TypedPipe.from(TextLine("/"))

    val (pipeline, op) = beamPlan(source)

    assert(op.isInstanceOf[BeamOp.Source[String]])
    assert(op.run(pipeline) == op.run(pipeline))
  }

  test("BeamOp caching: TransformBeamOp") {
    val pipe = TypedPipe.from(0 to 5).filter(_ % 2 == 0)

    val (pipeline, op) = beamPlan(pipe)

    assert(op.isInstanceOf[BeamOp.TransformBeamOp[Int, Int]])
    assert(op.run(pipeline) == op.run(pipeline))
  }

  test("map") {
    beamMatchesSeq(
      TypedPipe.from(0 to 5).map(_ * 2),
      Seq(0, 2, 4, 6, 8, 10)
    )
  }

  test("flatMap") {
    beamMatchesSeq(
      TypedPipe.from(0 to 3).flatMap(x => 0 to x),
      Seq(0, 0, 1, 0, 1, 2, 0, 1, 2, 3)
    )
  }

  test("mapValues") {
    beamMatchesSeq(
      TypedPipe.from(0 to 3).map(x => (x, x)).mapValues(_ * 2),
      Seq((0, 0), (1, 2), (2, 4), (3, 6))
    )
  }

  test("flatMapValues") {
    beamMatchesSeq(
      TypedPipe.from(0 to 2).map(x => (x, x)).flatMapValues(x => 0 to x),
      Seq((0, 0), (1, 0), (1, 1), (2, 0), (2, 1), (2, 2))
    )
  }

  test("filter") {
    beamMatchesSeq(
      TypedPipe.from(0 to 10).filter(x => x % 2 == 0),
      Seq(0, 2, 4, 6, 8, 10)
    )
  }

  test("filterKeys") {
    beamMatchesSeq(
      TypedPipe.from(0 to 10).map(x => (x, x)).filterKeys(x => x % 2 == 1),
      Seq((1, 1), (3, 3), (5, 5), (7, 7), (9, 9))
    )
  }

  test("mapGroup") {
    beamMatchesSeq(
      TypedPipe
        .from(Seq(5, 3, 2, 0, 1, 4))
        .map(x => x.toDouble)
        .groupAll
        .aggregate(AveragedValue.aggregator),
      Seq(((), 2.5))
    )
  }

  test("sortedMapGroup") {
    beamMatchesSeq(
      TypedPipe
        .from(Seq(5, 3, 2, 6, 1, 4))
        .groupBy(_ % 2)
        .sorted(Ordering[Int].reverse)
        .foldLeft(0)((a, b) => a * 10 + b),
      Seq((0, 642), (1, 531))
    )
  }

  test("sortedTake") {
    beamMatchesSeq(
      TypedPipe
        .from(Seq(5, 3, 2, 0, 1, 4))
        .map(x => x.toDouble)
        .groupAll
        .sortedReverseTake(3)
        .flatMap(_._2),
      Seq(5.0, 4.0, 3.0)
    )
  }

  test("bufferedTake") {
    beamMatchesSeq(
      TypedPipe
        .from(1 to 50)
        .groupAll
        .bufferedTake(100)
        .map(_._2),
      1 to 50,
      Config(Map("cascading.aggregateby.threshold" -> "100"))
    )
  }

  test("SumByLocalKeys") {
    beamMatchesSeq(
      TypedPipe
        .from(0 to 5)
        .map(x => (x, x))
        .flatMapValues(x => 0 to x)
        .sumByLocalKeys(new Semigroup[Int] {
          override def plus(x: Int, y: Int): Int = x + y
        }),
      Seq((0, 0), (1, 1), (2, 3), (3, 6), (4, 10), (5, 15)),
      Config.empty.setMapSideAggregationThreshold(5)
    )
  }

  test("HashJoin") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        leftPipe.hashJoin(rightPipe)
      },
      Seq((0, (0, 0)), (0, (0, 3)), (0, (1, 0)), (0, (1, 3)))
    )
  }

  test("HashLeftJoin") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        leftPipe.hashLeftJoin(rightPipe)
      },
      Seq(
        (0, (0, Some(0))),
        (0, (0, Some(3))),
        (0, (1, Some(0))),
        (0, (1, Some(3))),
        (1, (1, None)),
        (3, (3, None))
      )
    )
  }

  test("InnerJoin") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        leftPipe.join(rightPipe)
      },
      Seq((0, (0, 0)), (0, (0, 3)), (0, (1, 0)), (0, (1, 3)))
    )
  }

  test("LeftJoin") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        leftPipe.leftJoin(rightPipe)
      },
      Seq(
        (0, (0, Some(0))),
        (0, (0, Some(3))),
        (0, (1, Some(0))),
        (0, (1, Some(3))),
        (1, (1, None)),
        (3, (3, None))
      )
    )
  }

  test("Multiple LeftJoins") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))

        val thirdPipe: TypedPipe[(Int, String)] = TypedPipe.from(Seq((0, "a"), (1, "b")))

        leftPipe.join(rightPipe).leftJoin(thirdPipe)
      },
      Seq(
        (0, ((0, 0), Some("a"))),
        (0, ((0, 3), Some("a"))),
        (0, ((1, 0), Some("a"))),
        (0, ((1, 3), Some("a")))
      )
    )
  }

  test("Multiple Joins") {
    beamMatchesSeq(
      {
        val firstPipe: TypedPipe[(Int, Float)] = TypedPipe.from(Seq((0, 0f), (0, 1.5f), (1, 1.5f), (3, 3.5f)))
        val secondPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        val thirdPipe: TypedPipe[(Int, String)] = TypedPipe.from(Seq((0, "a"), (1, "b")))

        firstPipe.leftJoin(secondPipe).leftJoin(thirdPipe)
      },
      Seq(
        (0, ((0f, Some(0)), Some("a"))),
        (0, ((0f, Some(3)), Some("a"))),
        (0, ((1.5f, Some(0)), Some("a"))),
        (0, ((1.5f, Some(3)), Some("a"))),
        (1, ((1.5f, None), Some("b"))),
        (3, ((3.5f, None), None))
      )
    )
  }

  test("RightJoin") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        leftPipe.rightJoin(rightPipe)
      },
      Seq(
        (0, (Some(0), 0)),
        (0, (Some(0), 3)),
        (0, (Some(1), 0)),
        (0, (Some(1), 3)),
        (2, (None, 2)),
        (2, (None, 3))
      )
    )
  }

  test("OuterJoin") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        leftPipe.outerJoin(rightPipe)
      },
      Seq(
        (0, (Some(0), Some(0))),
        (0, (Some(0), Some(3))),
        (0, (Some(1), Some(0))),
        (0, (Some(1), Some(3))),
        (1, (Some(1), None)),
        (3, (Some(3), None)),
        (2, (None, Some(2))),
        (2, (None, Some(3)))
      )
    )
  }

  test("CoGroup") {
    beamMatchesSeq(
      {
        val leftPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 1), (1, 1), (3, 3)))
        val rightPipe: TypedPipe[(Int, Int)] = TypedPipe.from(Seq((0, 0), (0, 3), (2, 2), (2, 3)))
        leftPipe.cogroup(rightPipe)((_, iter1, iter2) => Seq((iter1 ++ iter2).toSeq.sum).toIterator)
      },
      Seq(
        (0, 4),
        (1, 1),
        (2, 5),
        (3, 3)
      )
    )
  }

  test("Merge (++) two pipes") {
    val a = TypedPipe.from(Seq(5, 3, 2, 6, 1, 4))
    val b = TypedPipe.from(Seq(15, 13, 12, 16, 11, 14))

    beamMatchesSeq(
      a ++ b,
      Seq(1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16)
    )
  }

  test("Merge (++) many pipes") {
    val a = TypedPipe.from(Seq(5, 3, 2, 6, 1, 4))
    val b = TypedPipe.from(Seq(15, 13, 12, 16, 11, 14))
    val c = TypedPipe.from(Seq(25, 23, 22, 26, 21, 24))
    val d = TypedPipe.from(Seq(35, 33, 32, 36, 31, 34))

    beamMatchesSeq(
      a ++ b ++ c ++ d,
      Seq(1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16, 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36)
    )
  }

  private def getContents(path: String, prefix: String): List[String] =
    new File(path).listFiles.flatMap { file =>
      if (file.getPath.startsWith(prefix)) {
        Source.fromFile(file).getLines().flatMap(line => line.split("\\s+").toList)
      } else List.empty[String]
    }.toList

  private def removeDir(path: String): Unit = {
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
      if (file.exists && !file.delete)
        sys.error(s"Unable to delete ${file.getAbsolutePath}")
    }
    deleteRecursively(new File(path))
  }
}
