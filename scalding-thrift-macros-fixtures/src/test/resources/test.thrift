namespace java com.twitter.scalding.thrift.macros.thrift
#@namespace scala com.twitter.scalding.thrift.macros.scalathrift

enum TestEnum
{
  Zero = 0,
  One = 1,
  Two = 2,
  Large = 70000,
  Huge = 2147483647
}

struct TestStruct {
 1: required string a_string,
 2: optional i32 a_i32,
 3: required string a_second_string
}

struct TestLists {
 1: required list<bool> a_bool_list,
 2: required list<byte> a_byte_list,
 3: required list<i16> a_i16_list,
 4: required list<i32> a_i32_list,
 5: required list<i64> a_i64_list,
 6: required list<double> a_double_list,
 7: required list<string> a_string_list,
 8: required list<TestStruct> a_struct_list,
 9: required list<list<string>> a_list_list,
 10: required list<set<string>> a_set_list,
 11: required list<map<i32,i32>> a_map_list
}

struct TestMaps {
 1: required map<bool,string> a_bool_map,
 2: required map<byte,double> a_byte_map,
 3: required map<i16,i64> a_i16_map,
 4: required map<i32,i32> a_i32_map,
 5: required map<i64,i16> a_i64_map,
 6: required map<double,byte> a_double_map,
 7: required map<string,bool> a_string_map,
 8: required map<TestStruct,list<string>> a_struct_map,
 9: required map<list<string>,TestStruct> a_list_map,
 10: required map<set<string>,set<string>> a_set_map,
 11: required map<map<i32,i32>,map<i32,i32>> a_map_map,
}

struct TestSerializationOrderItem{
 1: required string a_string
}

struct TestSerializationOrder{
 1: required set<TestSerializationOrderItem> a_struct_set
}

struct TestSets{
 1: required set<bool> a_bool_set,
 2: required set<byte> a_byte_set,
 3: required set<i16> a_i16_set,
 4: required set<i32> a_i32_set,
 5: required set<i64> a_i64_set,
 6: required set<double> a_double_set,
 7: required set<string> a_string_set,
 8: required set<TestStruct> a_struct_set,
 9: required set<list<string>> a_list_set,
 10: required set<set<string>> a_set_set,
 11: required set<map<i32,i32>> a_map_set
}

struct TestTypes{
 1: required bool a_bool,
 2: required byte a_byte,
 3: required i16 a_i16,
 4: required i32 a_i32,
 5: required i64 a_i64,
 6: required double a_double,
 7: required string a_string,
 8: required TestEnum a_enum,
 9: required binary a_binary
}

struct TestOptionTypes{
 1: optional bool a_bool,
 2: optional byte a_byte,
 3: optional i16 a_i16,
 4: optional i32 a_i32,
 5: optional i64 a_i64,
 6: optional double a_double,
 7: optional string a_string,
 8: optional TestEnum a_enum,
 9: optional binary a_binary
}

union TestUnion{
 1:  list<i32> a_i32_list,
 2:  set<double> a_double_set,
 3:  TestStruct a_struct
}

struct A {
  1: optional string _a
  7: optional i64 g
  8: optional i64 abcdEE
}


