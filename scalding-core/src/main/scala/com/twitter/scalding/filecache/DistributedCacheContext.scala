package com.twitter.scalding.filecache

trait DistributedCacheContextLike {
  implicit val distributedCache: DistributedCache
}

/**
 * This trait must be mixed into any scalding job class that uses DistributedCacheFile, as it
 * provides the DistributedCache instance needed to complete the registration
 */
trait DistributedCacheContext extends DistributedCacheContextLike {
  @transient
  implicit lazy val distributedCache: DistributedCache = new HadoopDistributedCache
}

