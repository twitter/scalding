package com.twitter.scalding.db.macros

import org.mockito.Mockito.{ reset, when }
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.exceptions.TestFailedException
import org.scalatest.mock.MockitoSugar

import cascading.tuple.{ Fields, Tuple, TupleEntry }

import com.twitter.scalding._
import com.twitter.scalding.db._
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }

import java.sql.{ ResultSet, ResultSetMetaData }
import java.util.Date

class VerticaJdbcMacroUnitTests extends WordSpec with Matchers with MockitoSugar {

  "Produces the VerticaRowSerializer" should {

    VerticaRowSerializerProvider[Demographics]
    VerticaRowSerializerProvider[User]
    VerticaRowSerializerProvider[User2]
    VerticaRowSerializerProvider[CaseClassWithDate]
    VerticaRowSerializerProvider[CaseClassWithOptions]
    VerticaRowSerializerProvider[ExhaustiveJdbcCaseClass]
    assert(true != false)
  }
}
