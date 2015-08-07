# Fields API: Reduce Functions of GroupBuilder

[com.twitter.scalding.GroupBuilder](https://github.com/twitter/scalding/blob/develop/scalding-core/src/main/scala/com/twitter/scalding/GroupBuilder.scala?source=cc)

### mapStream

Full access to the Iterator of values. Avoid it if you can as this cannot be optimized much

### scanLeft

A particular, but common, pattern to processes an iterator and return a new iterator.

### foldLeft

Exactly like the above, but we only keep the last value.

### reduce / mapReduceMap

Preferred operations, which are partially executed on the mappers.
