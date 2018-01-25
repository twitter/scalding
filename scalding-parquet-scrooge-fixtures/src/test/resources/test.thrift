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

namespace java com.twitter.scalding.parquet.scrooge.thrift_java.test
#@namespace scala com.twitter.scalding.parquet.scrooge.thrift_scala.test

struct TestListsInMap {
  1: string name,
  2: map<list<string>,list<string>> names,
}

struct Name {
  1: required string first_name,
  2: optional string last_name
}

struct Address {
  1: string street,
  2: required string zip
}

struct AddressWithStreetWithDefaultRequirement {
  1: string street,
  2: required string zip
}

struct Phone {
  1: string mobile
  2: string work
}

struct TestPerson {
  1: required Name name,
  2: optional i32 age,
  3: Address address,
  4: string info
}


struct RequiredMapFixture {
  1: optional string name,
  2: required map<string,string> mavalue
}

struct RequiredListFixture {
  1: optional string info,
  2: required list<Name> names
}

struct RequiredSetFixture {
  1: optional string info,
  2: required set<Name> names
}

struct RequiredPrimitiveFixture {
  1: required bool test_bool,
  2: required byte test_byte,
  3: required i16 test_i16,
  4: required i32 test_i32,
  5: required i64 test_i64,
  6: required double test_double,
  7: required string test_string,
  8: optional string info_string
}


struct StructWithReorderedOptionalFields {
  3: optional i32 fieldThree,
  2: optional i32 fieldTwo,
  1: optional i32 fieldOne,
}

struct StructWithIndexStartsFrom4 {
  6: required Phone phone
}

struct StructWithExtraField {
  3: required Phone extraPhone,
  6: required Phone phone
}


struct TestPersonWithRequiredPhone {
  1: required Name name,
  2: optional i32 age,
  3: required Address address,
  4: optional string info,
  5: required Phone phone
}

struct TestPersonWithAllInformation {
   1: required Name name,
   2: optional i32 age,
   3: required Address address,
   4: optional Address working_address,
   5: optional string info,
   6: required map<string,Phone> phone_map,
   7: optional set<string> interests,
   8: optional list<string> key_words
}

struct TestMapComplex{
  1: required map<Phone,Address> phone_address_map
}

struct TestMapBinary{
  1: required map<string,binary> string_binary_map
}

struct TestMapPrimitiveKey {
  1: required map<i16,string> short_map,
  2: required map<i32,string> int_map,
  3: required map<byte,string> byt_map,
  4: required map<bool,string> bool_map,
  5: required map<i64,string> long_map,
  6: required map<double,string> double_map,
  7: required map<string,string> string_map;
}

struct TestOptionalMap {
   1: optional map<i16,string> short_map,
   2: optional map<i32,string> int_map,
   3: optional map<byte,string> byt_map,
   4: optional map<bool,string> bool_map,
   5: optional map<i64,string> long_map,
   6: optional map<double,string> double_map,
   7: optional map<string,string> string_map
}

struct TestListPrimitive {
  1: required list<i16> short_list,
  2: required list<i32> int_list,
  3: required list<i64> long_list,
  4: required list<byte> byte_list,
  5: required list<string> string_list,
  6: required list<bool> bool_list,
  7: required list<double> double_list,
}

struct TestSetPrimitive {
  1: required set<i16> short_list,
  2: required set<i32> int_list,
  3: required set<i64> long_list,
  4: required set<byte> byte_list,
  5: required set<string> string_list,
  6: required set<bool> bool_list,
  7: required set<double> double_list
}

struct TestMapPrimitiveValue {
  1: required map<string,i16> short_map,
  2: required map<string,i32> int_map,
  3: required map<string,byte> byt_map,
  4: required map<string,bool> bool_map,
  5: required map<string,i64> long_map,
  6: required map<string,double> double_map,
  7: required map<string,string> string_map
}

union TestUnion {
  1: TestPerson first_person
  2: TestMapComplex second_map
}

enum Operation {
  ADD = 1,
  SUBTRACT = 2,
  MULTIPLY = 3,
  DIVIDE = 4
}

struct TestFieldOfEnum{
 1: required Operation op
 2: optional Operation op2
}

struct StringAndBinary {
  1: required string s
  2: required binary b
}

#fixture fox nested structures
struct NestedList {
  1: required list<list<Address>> rll
  2: required list<list<list<Address>>> rlll
  3: optional list<list<Address>> oll
  4: optional list<list<list<Address>>> olll
  5: list<list<Address>> ll
  6: list<list<list<Address>>> lll
}

struct ListNestMap {
  1: required list<map<Phone, Address>> rlm
  2: required list<list<map<Phone, Address>>> rllm
  3: optional list<map<Phone, Address>> olm
  4: optional list<list<map<Phone, Address>>> ollm
  5: list<map<Phone, Address>> lm
  6: list<list<map<Phone, Address>>> llm
}

struct ListNestSet {
   1: required list<set<Address>> rls
   2: required list<list<set<Address>>> rlls
   3: optional list<set<Address>> ols
   4: optional list<list<set<Address>>> olls
   5: list<set<Address>> ls
   6: list<list<set<Address>>> lls
}

struct ListNestEnum {
   1: required list<Operation> rle
}

struct MapNestMap {
  1: required map<map<Phone, Address>, map<Address, Phone>> rmm
  2: required map<map<map<Phone,Address>, Address>, map<Address, Phone>> rmmm
  3: optional map<map<Phone, Address>, map<Address, Phone>> omm
  4: optional map<map<map<Phone,Address>, Address>, map<Address, Phone>> ommm
  5: map<map<Phone, Address>, map<Address, Phone>> mm
  6: map<map<map<Phone,Address>, Address>, map<Address, Phone>> mmm
}

struct MapNestList {
  1: required map<list<Phone>, list<Address>> rml
  2: required map<list<list<Phone>>, list<list<Address>>> rmll
  3: optional map<list<Phone>, list<Address>> oml
  4: optional map<list<list<Phone>>, list<list<Address>>> omll
  5: map<list<Phone>, list<Address>> ml
  6: map<list<list<Phone>>, list<list<Address>>> mll
}

struct MapNestSet {
  1: required map<set<Phone>, set<Address>> rms
  2: required map<set<set<Phone>>, set<set<Address>>> rmss
  3: optional map<set<Phone>, set<Address>> oms
  4: optional map<set<set<Phone>>, set<set<Address>>> omss
  5: map<set<Phone>, set<Address>> ms
  6: map<set<set<Phone>>, set<set<Address>>> mss
}

struct SetNestSet {
  1: required set<set<Address>> rss
  2: required set<set<set<Address>>> rsss
  3: optional set<set<Address>> oss
  4: optional set<set<set<Address>>> osss
  5: set<set<Address>> ss
  6: set<set<set<Address>>> sss
}

struct SetNestList {
   1: required set<list<Address>> rsl
   2: required set<set<list<Address>>> rssl
   3: optional set<list<Address>> osl
   4: optional set<set<list<Address>>> ossl
   5: set<list<Address>> sl
   6: set<set<list<Address>>> ssl
}

struct SetNestMap {
  1: required set<map<Phone, Address>> rsm
  2: required set<set<map<Phone, Address>>> rssm
  3: required set<set<list<list<map<Phone, Address>>>>> rssllm
  4: optional set<map<Phone, Address>> osm
  5: optional set<set<map<Phone, Address>>> ossm
  6: optional set<set<list<list<map<Phone, Address>>>>> ossllm
  7: set<map<Phone, Address>> sm
  8: set<set<map<Phone, Address>>> ssm
  9: set<set<list<list<map<Phone, Address>>>>> ssllm
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

union UnionV2 {
  1: AString aString,
  2: ALong aLong,
  3: ABool aNewBool
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
