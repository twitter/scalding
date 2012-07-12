# Scalding #

### Version 0.7.1 ###
* ISSUE 124: Add crossWithSmaller and use it in normalize.
* ISSUE 123: Adds generated classes that allow tuple concatenation.

### Version 0.7.0 ###
* ISSUE 122: Upgrade cascading to version 2.0.2 and maple to 0.2.1
* ISSUE 119: Iterator fix
* ISSUE 118: Specialized tuplegetter
* ISSUE 117: Job conf keys
* ISSUE 116: Feature/flatten
* ISSUE 115: Fold scan init fix
* ISSUE 114: Feature/scald update
* ISSUE 113: Make sure SequenceFile uses the fields list if one is passed in
* ISSUE 110: Upgrade Kryo to 2.16.
* ISSUE 107: Add a method to GroupBuilder to force all aggregation on the reducers.
* ISSUE 105: Feature/head fix
* ISSUE 104: Feature/default date improvements
* ISSUE 103: Feature/case case pack
* ISSUE 100: Adds trivial UnitGroup
* ISSUE 99: Fix build breakage and add tutorial run script.

### Version 0.6.0 ###
* ISSUE 98: Feature/tpipe stream
* ISSUE 97: Add the user input to the unknown mode error message
* ISSUE 95: Feature/buffer cleanup
* ISSUE 94: Feature/tpipe
* ISSUE 93: Upgrade to cascading 2.0.0 and maple 0.2.0
* ISSUE 92: Feature/dsl refactor

### Version 0.5.4 ###
* ISSUE 91: Upgrade to cascading wip-310 and maple 0.1.10
* ISSUE 90: Test against multiple JDKs on travis-ci.org
* ISSUE 88: Bump sbt-assembly version from 0.7.3 to 0.8.1
* ISSUE 87: Header Lines support in DelimitedScheme
* ISSUE 84: Add packTo and unpackTo
* ISSUE 83: Adds sum and product to Monoid and Ring
* ISSUE 79: Updates to sbt 0.11.3 to match Travis CI

### Version 0.5.3 ###
* ISSUE 77: Add a scalding multi source tap that has a proper unique identifier

### Version 0.5.2 ###
* ISSUE 74: Upgrade cascading to wip-291 and maple to 0.1.7

### Version 0.5.1 ###
* ISSUE 73: Feature/comparator prop
* ISSUE 72: Upgrade to cascading wip-288 and maple 0.1.5
* ISSUE 71: Fixes an issue due to type erasure in KryoHadoopSerialization for scala 2.9.1
* ISSUE 70: Upgrade scalding to use cascading wip-286.
* ISSUE 69: Feature/more kryo tests

### Version 0.5.0 ###
* ISSUE 67: Upgrade cascading to wip-281.
* ISSUE 66: Allow default time zone in DefaultDateRangeJob.
* ISSUE 65: Fixed the error message thrown by FileSource.validateTaps.
* ISSUE 62: Kryo Upgrade to 2.04
* ISSUE 60: Feature/abstract algebra
* ISSUE 52: Feature/cogroup builder
* ISSUE 51: Feature/headfix

### Version 0.4.1 ###
* ISSUE 42: Feature/iterable source
* ISSUE 41: Adds blockJoinWithSmaller to JoinAlgorithms.
* ISSUE 39: Adding default value to pivot

### Version 0.4.0 ###
* ISSUE 38: Fix bug with hash code collisions of Source objects
* ISSUE 36: Some cleanups of reduce and Operations
* ISSUE 35: Split RichPipe join methods into their own trait
* ISSUE 34: Adds Pivot/Unpivot
* ISSUE 33: Add pack and unpack methods to RichPipe
* ISSUE 32: Refactors reducer setting into RichPipe
* ISSUE 31: Implemented Mode.fileExists
* ISSUE 28: Simplifies TupleConverter

### Version 0.3.5 ###
* ISSUE 21: move JobTest into main
* ISSUE 20: Adding a source for the most recent good date path.
