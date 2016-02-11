package com.twitter.scalding

package object parquet {
  type ParquetValueScheme[T] = com.twitter.scalding.parquet.cascading.ParquetValueScheme[T]

  object ParquetValueScheme {
    type Config[T] = com.twitter.scalding.parquet.cascading.ParquetValueScheme.Config[T]
  }
}

