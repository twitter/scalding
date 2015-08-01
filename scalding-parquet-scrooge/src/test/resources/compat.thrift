/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace java com.twitter.scalding.parquet.scrooge.thrift_java.test.compat
#@namespace scala com.twitter.scalding.parquet.scrooge.thrift_scala.test.compat

struct StructV1 {
  1: required string name
}
struct StructV2 {
  1: required string name,
  2: optional string age
}
struct StructV3 {
  1: required string name,
  2: optional string age,
  3: optional string gender
}

struct StructV4WithExtracStructField {
  1: required string name,
  2: optional string age,
  3: optional string gender,
  4: optional StructV3 addedStruct
}

struct RenameStructV1 {
  1: required string nameChanged
}

enum NumberEnum {
  ONE = 1,
  TWO = 2,
  THREE = 3
}

enum NumberEnumWithMoreValue {
  ONE = 1,
  TWO = 2,
  THREE = 3,
  FOUR = 4
}

struct StructWithEnum {
 1: required NumberEnum num
}

struct StructWithMoreEnum {
 1: required NumberEnumWithMoreValue num
}

struct TypeChangeStructV1{
  1: required i16 name
}

struct OptionalStructV1{
  1: optional string name
}

struct DefaultStructV1{
  1: string name
}

struct AddRequiredStructV1{
  1: required string name,
  2: required string anotherName
}

struct MapStructV1{
  1: required map<StructV1,string> map1
}

struct MapValueStructV1{
  1: required map<string,StructV1> map1
}

struct MapStructV2{
  1: required map<StructV2,string> map1
}

struct MapValueStructV2{
  1: required map<string,StructV2> map1
}

struct MapAddRequiredStructV1{
  1: required map<AddRequiredStructV1,string> map1
}

struct MapWithStructValue {
  1: required map<string, StructV4WithExtracStructField> reqMap
}

struct MapWithPrimMapValue {
  1: required map<string, map<string, string>> reqMap
}

struct MapWithStructMapValue {
  1: required map<string, map<string, StructV4WithExtracStructField>> reqMap
}

struct SetStructV1{
  1: required set<StructV1> set1
}

struct SetStructV2{
  1: required set<StructV2> set1
}

struct ListStructV1{
  1: required list<StructV1> list1
}

struct ListStructV2{
  1: required list<StructV2> list1
}

struct AString {
  1: required string s
}

struct ALong {
  1: required i64 l
}

struct ABool {
  1: required bool b
}

union UnionV1 {
  1: AString aString,
  2: ALong aLong
}

union UnionV2 {
  1: AString aString,
  2: ALong aLong,
  3: ABool aNewBool
}

struct StructWithUnionV1 {
  1: required string name,
  2: required UnionV1 aUnion
}

struct StructWithUnionV2 {
  1: required string name,
  2: required UnionV2 aUnion
}

struct AStructThatLooksLikeUnionV2 {
  1: optional AString aString,
  2: optional ALong aLong,
  3: optional ABool aNewBool
}

struct StructWithAStructThatLooksLikeUnionV2 {
  1: required string name,
  2: required AStructThatLooksLikeUnionV2 aNotQuiteUnion
}

union UnionOfStructs {
  1: StructV3 structV3,
  2: StructV4WithExtracStructField structV4,
  3: ABool aNewBool
}

struct StructWithUnionOfStructs {
  1: required string name,
  2: required UnionOfStructs aUnion
}

struct StructWithOptionalUnionOfStructs {
  1: required string name,
  2: optional UnionOfStructs aUnion
}

struct StructWithRequiredUnionOfStructs {
  1: required string name,
  2: required UnionOfStructs aUnion
}

struct OptionalInsideRequired {
  1: required string name,
  2: required StructWithOptionalUnionOfStructs aStruct
}

struct RequiredInsideOptional {
  1: required string name,
  2: optional StructWithRequiredUnionOfStructs aStruct
}

union UnionStructUnion {
  1: StructV3 structV3
  2: StructWithUnionOfStructs structWithUnionOfStructs
  3: ALong aLong
}

union NestedUnion {
  1: StructV3 structV3
  2: UnionOfStructs unionOfStructs
  3: ALong aLong
}

union NestedNestedUnion {
  1: NestedUnion nestedUnion
  2: UnionV2 unionV2
}

struct StructWithNestedUnion {
  1: optional UnionOfStructs optUnionOfStructs
  2: required UnionOfStructs reqUnionOfStructs
  3: UnionOfStructs unspecifiedUnionOfStructs

  4: optional NestedUnion optNestedUnion
  5: required NestedUnion reqNestedUnion
  6: NestedUnion unspecifiedNestedUnion

  7: optional StructWithUnionV2 optStructWithUnionV2
  8: required StructWithUnionV2 reqStructWithUnionV2
  9: StructWithUnionV2 unspecifiedStructWithUnionV2

  10: optional UnionStructUnion optUnionStructUnion
  11: required UnionStructUnion reqUnionStructUnion
  12: UnionStructUnion unspecifiedUnionStructUnion
}

struct MapWithUnionKey {
  1: optional map<UnionOfStructs, StructV3> optMapWithUnionKey
  2: required map<UnionOfStructs, StructV3> reqMapWithUnionKey
}

struct MapWithUnionValue {
  1: optional map<StructV3, UnionOfStructs> optMapWithUnionValue
  2: required map<StructV3, UnionOfStructs> reqMapWithUnionValue
}

struct ListOfUnions {
  1: optional list<UnionOfStructs> optListUnion
  2: required list<UnionOfStructs> reqListUnion
}

struct EmptyStruct {

}

struct NestedEmptyStruct {
  1: required EmptyStruct required_empty
  2: optional EmptyStruct optional_empty
}
