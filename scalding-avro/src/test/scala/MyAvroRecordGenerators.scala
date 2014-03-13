package com.twitter.scalding.avro

import scala.collection.JavaConverters._

import org.apache.avro.Schema
import org.apache.avro.Schema.Field

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._

/**
 * ScalaCheck generators to create arbitrary `MyAvroRecord` objects.
 */
object MyAvroRecordGenerators {

  val myAvroGen: Gen[MyAvroRecord] = for {
    id <- arbitrary[Long]
    bookingId <- arbitrary[String]
    entryDate <- arbitrary[String]
    result <- arbitrary[Int]
    a <- arbitrary[String]
    b <- arbitrary[String]
    c <- arbitrary[String]
    d <- arbitrary[String]
    e <- arbitrary[String]
    f <- arbitrary[String]
    g <- arbitrary[Int]
    h <- arbitrary[String]
    price <- arbitrary[Double]
    j <- arbitrary[Double]
    k <- arbitrary[String]
    l <- arbitrary[Double]
    m <- arbitrary[Int]
    n <- arbitrary[Int]
    o <- arbitrary[Int]
    p <- arbitrary[String]
    q <- arbitrary[Int]
    r <- arbitrary[Int]
    s <- arbitrary[Int]
    t <- arbitrary[Int]
    u <- arbitrary[Int]
    v <- arbitrary[Int]
    w <- arbitrary[Double]
    x <- arbitrary[Double]
    y <- arbitrary[Double]
    z <- arbitrary[Double]
    aa <- arbitrary[Double]
    bb <- arbitrary[Double]
    cc <- arbitrary[Double]
    dd <- arbitrary[Int]
    ee <- arbitrary[Int]
    ff <- arbitrary[Int]
    gg <- arbitrary[Int]
    hh <- arbitrary[Int]
    ii <- arbitrary[Int]
  } yield new MyAvroRecord(id, bookingId, entryDate, result, a, b, c,
      d, e, f, g, h, price, j, k, l, m, n, o, p, q, r, s, t, u, v,
      w, x, y, z, aa, bb, cc, dd, ee, ff, gg, hh, ii)

  lazy val schemaGen: Gen[Schema] = for {
    name <- identifier
    doc <- arbitrary[String]
    namespace <- arbitrary[String]
    fields <- listOf(arbitrary[Field])
  } yield {
    val schema = Schema.createRecord(name, doc, namespace, false)
    schema.setFields(fields.asJava)
    schema
  }

  implicit val arbitraryField: Arbitrary[Field] = Arbitrary {
    for {
      name <- identifier
      schemaType <- oneOf(Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.BOOLEAN)
      doc <- arbitrary[String] suchThat (s => s.length > 0)
      //TODO defaultValue <- arbitrary[JsonNode]
    } yield {
      val schemaString =
        """
          |{
          |"type" : "%s"
          |}
        """.stripMargin.format(schemaType.getName)

      val schema = new Schema.Parser().parse(schemaString)
      val field = new Field(name, schema, doc, null)
      field
    }
  }
}
