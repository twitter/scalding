# Field Rules

## Map-phase
In the map phase (map/flatMap/pack/unpack) the rule is: if the target fields are new (disjoint from the input fields), they are appended.  If source or target fields are a subset of the other, only the results are kept.  Otherwise, you get an exception at flow planning stage (there is some overlap but not subset relationship).

If you use mapTo/flatMapTo/packTo/unpackTo, only the results are kept.

## GroupBy
In the groupBy, the keys are always kept and only the target fields.  So, `groupBy('x) { _.sum('y -> 'ys).sum('z -> 'zs) }` will return a pipe with three columns:
`('x, 'ys, 'zs)`.

## Joins

Joins keep all the fields.  For inner joins, the field names can collide on the fields you are joining, and in that case only one copy is kept in the result.  Otherwise, all field names must be distinct.

## Cascading Experts

The documentation in [FieldConversions.scala](https://github.com/twitter/scalding/blob/master/src/main/scala/com/twitter/scalding/FieldConversions.scala#L60) might be helpful for Cascading experts.
```
  /**
  * Rather than give the full power of cascading's selectors, we have
  * a simpler set of rules encoded below:
  * 1) if the input is non-definite (ALL, GROUP, ARGS, etc...) ALL is the output.
  *      Perhaps only fromFields=ALL will make sense
  * 2) If one of from or to is a strict super set of the other, SWAP is used.
  * 3) If they are equal, REPLACE is used.
  * 4) Otherwise, ALL is used.
  */
```
