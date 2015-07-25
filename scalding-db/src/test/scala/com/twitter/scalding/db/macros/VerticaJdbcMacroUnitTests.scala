/*
Copyright 2015 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
