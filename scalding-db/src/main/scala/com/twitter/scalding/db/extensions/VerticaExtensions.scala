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

package com.twitter.scalding.db.extensions

import com.twitter.scalding.db._

object VerticaExtensions {
  def verticaMutator: PartialFunction[DBColumnDefinition, DBColumnDefinition] = {
    case t @ DBColumnDefinition(BIGINT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DBColumnDefinition(INT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DBColumnDefinition(SMALLINT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DBColumnDefinition(BOOLEAN, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DBColumnDefinition(TINYINT, _, _, None, _, _) => t.copy(sizeOpt = None)
    case t @ DBColumnDefinition(DOUBLE, _, _, _, _, _) => t.copy(sqlType = SqlTypeName("DOUBLE PRECISION"))
  }
}
