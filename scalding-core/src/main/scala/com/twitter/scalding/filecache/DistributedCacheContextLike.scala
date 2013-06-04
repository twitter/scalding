package com.twitter.scalding.filecache

trait DistributedCacheContextLike {
  implicit val distributedCache: DistributedCache
}

trait DistributedCacheContext extends DistributedCacheContextLike {
  @transient
  implicit lazy val distributedCache: DistributedCache = new HadoopDistributedCache
}

