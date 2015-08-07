# Upgrading to 0.9.0

1. `def config` in Job does not accept a mode. Job has `Job.mode` use that if you need it.  `def listeners` in Job no longer accepts a mode either.
2. `sum` takes a type parameter in the fields API. `sum[Double]` is equivalent to the old behavior, but you might want `sum[Long]` or any other `T` that has an `algebird.Semigroup[T]`. Without the type parameter you get: `diverging implicit expansion for type com.twitter.algebird.Semigroup[T]`
3. TypedSink and Mappable need setter and converter defined. Using TupleSetter.asSubSetter, or TupleConverter.asSuperConverter can help here. (add better docs, please if you get confused).
4. RichDate parsing needs an implicit scalding.DateParser. Job has one in scope that follows the old rules (minus natty), but you may need to set an implicit DateParser outside of a Job. (See DateParser.default).
5. `JsonLine` has been extracted into `scalding-json` module.

These [sed rules](https://gist.github.com/johnynek/6632488) may help.
