namespace java com.twitter.scalding.parquet.thrift_java.test
#@namespace scala com.twitter.scalding.parquet.thrift_scala.test

struct Name {
  1: required string first_name,
  2: optional string last_name
}

struct Address {
  1: string street,
  2: required string zip
}
