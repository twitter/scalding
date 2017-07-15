# Scalding #

### Version 0.17.2 ###
This version is basically the same as 0.17.1 but backward compatible with 0.17.0.
* Revert memory estimator changes on 0.17.x branch: #1704
* Turn on mima checks on 0.17.x branch: #1706

### Version 0.17.1 ###
This version breaks backward compatibility with 0.17.0, don't use it, use 0.17.2 instead.
* Request for Scalding release 0.17.0: #1641
* make ordered serialization stable across compilations: #1664
* Remove unnecessary semicolon: #1668
* Add tailrec annotation: #1671
* Be more paranoid about Kryo registration order: #1673
* Update sbt version to 0.13.15: #1677
* Register all Boxed classes in Kryo: #1678
* Fix serialization of KryoHadoop: #1685
* Merge pull request #1686 from ttim/cherry_pick_0.17.x_changes
* Fix stack overflow in typedPipeMonoid.zero: #1688
* A couple of fixes into the 0.17.x branch: #1695
* Memory estimator changes to 0.17.x branch: #1700

### Version 0.17.0 ###
This is the first Scalding release that publishes artifacts for Scala 2.12!
* 2.12 releated updates: #1663, #1646
* Use reflection over Jobs to find serialized classes: #1654, #1662
* Simplify match statement and use collection.breakOut: #1661
* Add explicit types to implicit methods and values: #1660
* Reducer estimation size fixes: #1652, #1650, #1645, #1644
* Use Combined*SequenceFile for VKVS, WritableSequenceFileScheme, SequenceFileScheme: #1647
* Improve Vertica support in scalding-db: #1655
* Add andThen to Mappable: #1656
* Expand libjars globs in ScaldingShell to match the behavior of Tool: #1651
* Use Batched in Sketch production: #1648
* Pick up Algebird 0.13.0: #1640
* Added API for Execution/Config to work with DistributedCache: #1635
* Bump chill version to 0.8.3: #1634
* Fixes a bug in how we use this stack: #1632
* Upgrade build to sbt 0.13.13: #1629
* Generate Scalding microsite via sbt-microsites: #1623
* FileSource support for empty directories: #1622, #1618, #1613, #1611, #1591
* Clean up temporary files created by forceToDiskExecution: #1621
* Moving the repl in wonderland to a dedicated md file: #1614
* Update Scala and sbt version: #1610
* REFACTOR: Fixed some compilation warnings: #1604
* REFACTOR: Rename parameter to reflect expectation: #1601
* Add partitioned sources for Parquet thrift / scrooge: #1590
* Add a test for sortBy: #1594
* Create COMMITTERS.md: #1589
* Use ExecutionContext in Execution.from/fromTry: #1587
* Support custom parquet field name strategies: #1580
* Deprecate reflection-based JobTest apply method: #1578
* Use Caching for FlowDefExecution: #1581
* [parquet tuple macros] listType was deprecated in favor of listOfElements: #1579
* Use Batched to speed up CMS summing on mappers: #1575
* Remove a TypedPipeFactory wrapper which seems unneeded: #1576
* Make Writeable sources Mappable to get toIterator: #1573
* case class implicit children: #1569

### Version 0.16.0 ###

* Add tests around hashcode collisions : #1299
* Fix performance bug in TypedPipeDiff : #1300
* make serialization modules build on travis  : #1301
* Improve TypedParquetTuple  : #1303
* Add UnitOrderedSerialization  : #1304
* Revert "Add UnitOrderedSerialization"  : #1306
* Change groupRandomly & groupAll to use OrderedSerialization  : #1307
* Make test of Kmeans very very unlikely to fail  : #1310
* make LongThrift sources TypedSink  : #1313
* Fix testing VersionedKeyValSource#toIterator for non-Array[Byte] types  : #1314
* Make SketchJoin ordered serialization aware  : #1316
* Added a sealed trait ordered serializer. When it works its great. Not as reliable as we'd like. But hopefully restrictions on it will do the job  : #1320
* Add secondary sorting using ordered serialization  : #1321
* Bails out from the length calculation if we don't succeed often : #1322
* increased number of box instances to 250  : #1323
* Apply merge strategy for pom.properties files  : #1325
* Apply merge strategy for pom.xml files  : #1327
* Add a OrderedSerialization.viaTransform with no dependencies, and a BijectedOrderedSerialization in scalding core  : #1329
* Precompute int hashes  : #1330
* Hide the deprecated string error for getting ASCII bytes.  : #1332
* Change defaults for Scalding reducer estimator  : #1333
* Execution id code  : #1334
* Add line numbers at .group and .toPipe boundaries  : #1335
* Ordered Serialization macros for thrift  : #1338
* make some repl components extensible  : #1342
* Remove the bootstrap section  : #1346
* Fix the execution test : #1347
* Implement flatMapValues method  : #1348
* Consistent style in homepage example  : #1349
* Serialization folding  : #1351
* Collapses scalding-db packages  : #1353
* Merge scalding-macros into scalding-core  : #1355
* Migrate typedtext  : #1356
* Runtime reducer estimator  : #1358
* Update Build.scala  : #1361
* Allow overriding of hadoop configuration options for a single source/sink  : #1362
* Missing an extends Serializable, causes issues if capture Config's anywhere : #1365
* Fix TypedPipe.limit to be correct, if slighly slower  : #1366
* Fix scala.Function2 showing up in line numbers : #1367
* Drop with MacroGenerated from Fields macros  : #1370
* Fix deprecation warnings in TypedDelimited  : #1371
* Ianoc/revert changes around making file systems  : #1372
* Revert typed tsv behavior  : #1373
* A serialization error we were seeing in repl usage : #1376
* Add NullSink and test  : #1378
* Add monoid and semigroup for Execution  : #1379
* Upgrade parquet to 1.8.1  : #1380
* Upgrade sbt launcher script (sbt-extras)  : #1381
* Just move whitespace, add comments, simplify a few methods  : #1383
* Don't publish maple when doing 2.11 so we only publish it once -- needed for cross publishing to maven repo's  : #1386
* Support nesting Options in TypeDescriptor  : #1387
* Enable Scalding-REPL for Scala 2.11  : #1388
* Updates for some upstream fixes/changes  : #1390
* Remove use of hadoop version in estimators  : #1391
* Set hadoop version to dummy value  : #1392
* Handle no history case in RatioBasedEstimator  : #1393
* Inline parquet-scrooge  : #1395
* RatioBasedEstimator - fix threshold edge case, add tests  : #1397
* Fixes the scrooge generator tasks not to generate code in the compile target, we were publishing these  : #1399
* Ianoc/configure set converter  : #1400
* Change hash function in GroupRandomly  : #1401
* Improve logging in runtime reducer estimators  : #1402
* Add the type in ScroogeReadSupport  : #1403
* Adds a function to test if a sink exists at the version we created  : #1404
* add .groupWith method to TypedPipe  : #1406
* Add some return types  : #1407
* add counter verification logic  : #1409
* Runtime reducer estimator fixes  : #1411
* Make sure Execution.zip fails fast  : #1412
* When using WriteExecution and forceToDisk we can share the same flow def closer in construction  : #1414
* Cache the zipped up write executions  : #1415
* Fix DateOps "match may not be exhaustive" warning  : #1416
* Factor out repeated code into FutureCache  : #1417
* Fix lack of Externalizer in joins.  : #1421
* Adds much more line number information through the NoStackAndThen class  : #1423
* Requires a DateRange's "end" to be after its "start"  : #1425
* Scalding viz options  : #1426
* Fixes map-only jobs to accommodate both an lzo source and sink binary converter  : #1431
* Fix Readme travis link  : #1432
* Fixes docs wording  : #1433
* Don't squash the exception in history service when there's a failure  : #1434
* Log the exception in RatioBasedEstimator when there's a failure  : #1435
* make getBytesPerReducer support human readable values like 128m and 1g  : #1436
* Fixes minor KeyedList docs wording  : #1437
* Fix `readPathsFor` to use the `tz` argument  : #1439
* Scalding viz options  : #1440
* call Job.validate when running tests under JobTest  : #1441
* opt-in to calling Job.validate in JobTest  : #1444
* Fix bug with sketch joins and single keys  : #1451
* Fix incorrect usage of `percent`.  : #1455
* Add OrderedSerialization2 support in Matrix2.  : #1457
* Add InvalidSourceTap to catch all cases for no good path.  : #1458
* Cluster info and fs shell in repl  : #1462
* Update Scala version to 2.10.6  : #1463
* Fix median estimation  : #1464
* Makes the config transient in the KryoHadoop instanciator  : #1466
* Moves the default to 2.11  : #1467
* Adds Error Message to REPL when Current Directory Not Readable  : #1468
* SuccessFileSource: correctness for multi-dir globs  : #1470
* Limit task history fields consumed from hraven  : #1472
* Remove dependency on dfs-datastores  : #1473
* ScaldingILoop should enable one to pass in in/out  : #1475
* Switch Chat to Gitter  : #1477
* Add two functions that assist in testing a TypedPipe  : #1478
* Makes permission failures non-fatal when looking for .scalding_repl files  : #1479
* Update TypeDescriptor to explain that Option[String] is not supported  : #1480
* Remove a type parameter that doesn't seem to do anything  : #1481
* Utility for expanding libjars  : #1483
* Shouldn't skip hidden files, user can decide such things with their glob  : #1485
* Fix FileSystem.get issue  : #1487
* Remove dependency on parquet-cascading  : #1488
* Add withConfig api to allow running an execution with a transformed config  : #1489
* Call validateTaps in toIterator codepath  : #1490
* Update the build  : #1491
* Arg Descriptions/Help for Execution Apps  : #1492
* Fix issue #1429  : #1493
* Cache counters for stat updates  : #1495
* Pulls the core ExecutionTests back into scalding-core  : #1498
* Add a liftToTry function to Execution  : #1499
* Small improvements to the `Boxed.scala` module  : #1500
* Cache boxed classes  : #1501
* Fix unnecessary use of `.get` in `Globifier.scala`  : #1502
* Replace unintentional use of `Unit` with `()`  : #1503
* Fix unnecessary uses of `Option.get`  : #1506
* Utility methods for running Executions in parallel  : #1507
* Typed Mapside Reduce  : #1508
* Use `wartremover` to guard against careless use of `_.get`  : #1509
* Add in an API around cache isolation  : #1511
* Add implicit Ordering[RichDate]  : #1512
* Fix MultipleTextLineFiles source in JobTest  : #1513
* Add's support for sealed abstract classes  : #1518
* Update FixedPathSource to strip out '*' in paths ending with '/*' for writes  : #1520
* support for more formats to work with RichDate  : #1522
* WIP: Add forceToDisk parameter to hashJoin in TypedPipe  : #1529
* Fixing comments on partitioned delimited source  : #1530
* Remove weakly typed Source  : #1531
* Maple fix for HBaseTap  : #1532
* Add an enrichment for TypedPipe.inMemoryToList and use it in TypedPipeDiff test  : #1533
* Because, because... fun, the scala compiler has special naming rules it appears when there are leading underscores  : #1534
* Fix README examples link  : #1536
* Fixes Config to accommodate spaces in argument values  : #1537
* Add before() and after() to RichDate  : #1538
* Adds late tap validation for cases where race conditions cause it to fail  : #1540
* Fix Rounding Bug in RatioBasedEstimator  : #1542

### Version 0.15.0 ###
* Move OrderedSerialization into zero-dep scalding-serialization module #1289
* bump elephantbird to 4.8 #1292
* Fix OrderedSerialization for some forked graphs #1293
* Add serialization modules to aggregate list #1298

### Version 0.14.0 ###
* add .unit to Execution object #1189
* Override hashCode for Args #1190
* Put a value in a exception message #1191
* Add an exclusiveUpper method to DateRange #1194
* Covert LzoTextDelimited to Cascading scheme. #1179
* Remove Travis IRC notifications #1200
* add LookupJoin and LookupJoinTest changes from summingbird #1199
* Add a new ExecutionApp tutorial #1196
* Move main simple example to be the typed API, and put the .'s at the sta... #1193
* Add Execution.withArgs #1205
* Config/Cascading updater #1197
* Remove algebird serializers #1206
* remove warnings in CumulativeSum #1215
* Implicit execution context / easier switching between modes #1113
* add row l1 normalize #1214
* provide Args as an implicit val #1219
* call sourceConfInit when reading from taps in local mode #1228
* Add distinctCount and distinctValues helper methods to KeyedList. #1232
* import hygiene: remove unused imports and remove JavaConversions use #1239
* Swap hash and filename for filename-extension-sensitive code #1243
* Remove more unused imports #1240
* Provide useHdfsLocalMode for an easy switch to mapreduce local mode #1244
* upgrade scalacheck and scalatest #1246
* Optimize string and (hopefully) number comparisons a bit #1241
* Note the active FlowProcess for Joiners #1235
* Make sure Executions are executed at most once #1253
* Fix Config.getUniqueIDs #1254
* Add MustHasReducers trait. #1252
* Make sure the EvalCache thread isDaemon #1255
* Use non-regex split function #1251
* make InputSizeReducerEstimator work for any CompositeTap #1256
* TimePathedSource helper methods #1257
* Fix for reducer estimation not working correctly if withReducers is set to 1 reducer #1263
* Add make(dest) to TypedPipe #1217
* Fix SimpleDateFormat caching by default #1265
* upgrade sbt and sbt launcher script #1270
* Add TypedPipeDiff for comparing typed pipes #1266
* Change separator from \1 to \u0001 #1271
* Disable reducer estimation for map-only steps #1276
* Local sources support multiple paths #1275
* fix the spelling of the cumulativeSumTest file #1281
* Hydrate both sides of sampledCounts in skewJoinWithSmaller #1278
* Bijection 0.8.0, algebird 0.10.0, chill 0.6.0, scala 2.10.5 #1287
* Remove some deprecated items #1288

### Version 0.13.1 ###
* Back out 4 changes to be binary compatible: https://github.com/twitter/scalding/pull/1187
* Use java.util.Random instead of scala.util.Random: https://github.com/twitter/scalding/pull/1186
* Add Execution.failed: https://github.com/twitter/scalding/pull/1185
* Using a ConcurrentHashMap instead of a WeakHashMap to make the Stats behave in a correct manner: https://github.com/twitter/scalding/pull/1184
* Add applicative for Execution: https://github.com/twitter/scalding/pull/1181

### Version 0.13.0 ###
* Covert LzoTextDelimited to Cascading scheme.: https://github.com/twitter/scalding/pull/1179
* Make TraceUtil support versions of cascading older than 2.6: https://github.com/twitter/scalding/pull/1180
* Add support for more LzoTextDeilmited parameters in LzoTraits.: https://github.com/twitter/scalding/pull/1178
* Use latest algebird, bijection, chill, elephantbird, and scala 2.11.5: https://github.com/twitter/scalding/pull/1174
* Cascading 2.6 tracing: https://github.com/twitter/scalding/pull/1156
* use Cascading 2.6.1 and cascading-jdbc 2.6.0: https://github.com/twitter/scalding/pull/1110
* add reducer option to LookupJoin: https://github.com/twitter/scalding/pull/1160
* Add dump to ValuePipe in the REPL: https://github.com/twitter/scalding/pull/1157
* Ianoc/type descriptor: https://github.com/twitter/scalding/pull/1147
* Refactor around the macro definitions into 3 files. Both converter and setter support Options: https://github.com/twitter/scalding/pull/1145
* Fix a few random typos: https://github.com/twitter/scalding/pull/1144
* Fix two issues found by static analysis: https://github.com/twitter/scalding/pull/1143
* Add implicit helpers for numeric arguments: https://github.com/twitter/scalding/pull/1138
* Add a fields macro: https://github.com/twitter/scalding/pull/1132
* Ianoc/case class tuple converters: https://github.com/twitter/scalding/pull/1131
* Some minor changes, cleanup pulled from jco&#39;s macro branch: https://github.com/twitter/scalding/pull/1130
* Adds a typedjson source: https://github.com/twitter/scalding/pull/1129
* Pulls all external 3rdparty versions up to the top of the build file: https://github.com/twitter/scalding/pull/1128
* remove transitive pig and elephantbird dependencies for parquet-cascading: https://github.com/twitter/scalding/pull/1127
* Some minor clean up in the build file: https://github.com/twitter/scalding/pull/1123
* Ianoc/scalding 210: https://github.com/twitter/scalding/pull/1116
* Decrease test count: https://github.com/twitter/scalding/pull/1117
* Removes scala 2.9.3: https://github.com/twitter/scalding/pull/1106
* Fix some typos in TypedPipe docs, expand flatMap docs: https://github.com/twitter/scalding/pull/1115
* Implicit execution context / easier switching between modes: https://github.com/twitter/scalding/pull/1113
* Add more documentation to TypedPipe: https://github.com/twitter/scalding/pull/1111
* Update the README: https://github.com/twitter/scalding/pull/1114
* Fixed comment in LookupJoin.scala: https://github.com/twitter/scalding/pull/1108

### Version 0.12.0 ###
* Fix long compile time for MultiJoin helpers: https://github.com/twitter/scalding/pull/1109
* Allows reducer estimation to operate on all hfs taps: https://github.com/twitter/scalding/pull/1080
* Fix bufferedTake: https://github.com/twitter/scalding/pull/1107
* Generate methods for flattening the results of many joins: https://github.com/twitter/scalding/pull/1097
* Make TimePathedSource more configurable: https://github.com/twitter/scalding/pull/1105
* Adding DailyPrefixSuffixLzoTsv: https://github.com/twitter/scalding/pull/1082
* Option to select the fields for output in templatesource: https://github.com/twitter/scalding/pull/1061
* Add a DailySuffixMostRecentLzoProtobuf source: https://github.com/twitter/scalding/pull/1104
* Updates default scala version to 2.10.4: https://github.com/twitter/scalding/pull/1081
* MultiSourceTap hashcode: https://github.com/twitter/scalding/pull/1101
* scalding-core: merge flow step strategies to allow reducer estimation combined with other strategies: https://github.com/twitter/scalding/pull/1094
* Improve command line handling of the execution app: https://github.com/twitter/scalding/pull/1083
* More testing around the globifier with new properties: https://github.com/twitter/scalding/pull/1092
* Refactor JDBCSource to add compile-time info about type of DB: https://github.com/twitter/scalding/pull/1087
* Add a cumulative sum to KeyedList: https://github.com/twitter/scalding/pull/1085
* Add in failing test case: https://github.com/twitter/scalding/pull/1090
* Adds ability to also get the mode inside the Execution monad.: https://github.com/twitter/scalding/pull/1088
* Enforce invariant: mapGroup iterators all nonempty: https://github.com/twitter/scalding/pull/1072
* Allow PartitionSource to limit the number of open files: https://github.com/twitter/scalding/pull/1078
* append to Cascading frameworks system property instead of setting it directly: https://github.com/twitter/scalding/pull/1076
* Adds some output while assembly is building to keep travis happy: https://github.com/twitter/scalding/pull/1084
* Only request necessary hadoop configs in hraven reducer estimator: https://github.com/twitter/scalding/pull/1067
* Add parquet-scrooge sources: https://github.com/twitter/scalding/pull/1064
* Outer join handles case when both are empty: https://github.com/twitter/scalding/pull/1065
* Fix race in merging: https://github.com/twitter/scalding/pull/1063
* Add support for column projection to parquet sources: https://github.com/twitter/scalding/pull/1056
* Add typed version of RichPipe &#39;using&#39;: https://github.com/twitter/scalding/pull/1049
* Add getExecution/getOrElseExecution: https://github.com/twitter/scalding/pull/1062
* Change toIteratorExecution to toIterableExecution: https://github.com/twitter/scalding/pull/1058
* Cache Execution evaluations: https://github.com/twitter/scalding/pull/1057
* Add support for push down filters in parquet sources: https://github.com/twitter/scalding/pull/1050
* Add support for Fold: https://github.com/twitter/scalding/pull/1053
* move to use JobConf(true) for hadoop crazyness that causes host not foun...: https://github.com/twitter/scalding/pull/1051
* Disable Cascading update check.: https://github.com/twitter/scalding/pull/1048
* Respects -Dmapred.job.name when passed in on the command line: https://github.com/twitter/scalding/pull/1045
* Add some instances from Algebird: https://github.com/twitter/scalding/pull/1039
* Fix join.mapGroup issue: https://github.com/twitter/scalding/pull/1038
* Add a defensive .forceToDisk in Sketched: https://github.com/twitter/scalding/pull/1035
* Override toIterator for all Mappable with transformForRead: https://github.com/twitter/scalding/pull/1034
* Make sinkFields in TypedDelimited final.: https://github.com/twitter/scalding/pull/1032
* Fixed type of exception thrown by validateTaps: https://github.com/twitter/scalding/pull/1033
* Add default local maven repo to the resolver list: https://github.com/twitter/scalding/pull/1024
* Add an ExecutionApp trait for objects to skip the Job class: https://github.com/twitter/scalding/pull/1027
* Make each head pipe have a unique name: https://github.com/twitter/scalding/pull/1025
* Run REPL from SBT: https://github.com/twitter/scalding/pull/1021
* Add Config to openForRead: https://github.com/twitter/scalding/pull/1023
* Fix replConfig merging and evaluate values in Config.fromHadoop: https://github.com/twitter/scalding/pull/1015
* REPL Autoload file: https://github.com/twitter/scalding/pull/1009
* Fix hRaven Reducer Estimator: https://github.com/twitter/scalding/pull/1018
* Update Cascading JDBC Version.: https://github.com/twitter/scalding/pull/1016
* Some Execution fixes: https://github.com/twitter/scalding/pull/1007
* Refactor InputSizeReducerEstimator to correctly unroll MultiSourceTaps: https://github.com/twitter/scalding/pull/1017
* Fix issue #1011: Building develop branch fails: https://github.com/twitter/scalding/pull/1012
* hRaven Reducer Estimator: https://github.com/twitter/scalding/pull/996
* JsonLine should handle empty lines: https://github.com/twitter/scalding/pull/966
* Add comments for memory-related reduce operations.: https://github.com/twitter/scalding/pull/1006
* Add the remaining odds and ends to Execution[T]: https://github.com/twitter/scalding/pull/985
* Fix up the tests to run forked, and split across lots of travis builds: https://github.com/twitter/scalding/pull/993
* Typedpipe partition: https://github.com/twitter/scalding/pull/987
* Fix toIterator bug (#988): https://github.com/twitter/scalding/pull/990
* Basic reducer estimator support: https://github.com/twitter/scalding/pull/973
* Improve TypedSimilarity algorithm and update test.: https://github.com/twitter/scalding/pull/983
* Adds support for Counters inside the Execution Monad.: https://github.com/twitter/scalding/pull/982
* Make map/flatMap lazy on IterablePipe to address OOM: https://github.com/twitter/scalding/pull/981
* JsonLine: enable read transformation in test to get correct fields in sourceTap: https://github.com/twitter/scalding/pull/971
* Read and writable partitioned sources: https://github.com/twitter/scalding/pull/969
* Make an Execution[T] type, which is a monad, which makes composing Jobs easy.: https://github.com/twitter/scalding/pull/974
* Generalize handling of merged TypedPipes: https://github.com/twitter/scalding/pull/975
* Do not inherit from FileSource in LzoTraits: https://github.com/twitter/scalding/pull/976
* Make TypedPipe immutable: https://github.com/twitter/scalding/pull/968
* Adds an optional source: https://github.com/twitter/scalding/pull/963
* Add pipe1.join(pipe2) syntax in TypedAPI: https://github.com/twitter/scalding/pull/958
* Extending BddDsl for Typed API: https://github.com/twitter/scalding/pull/956
* VerticaJdbcDriver: https://github.com/twitter/scalding/pull/957
* fix the example usage in JDBCSource: https://github.com/twitter/scalding/pull/955
* Push back off ec2 requiring sudo, build failures are a nightmare: https://github.com/twitter/scalding/pull/953
* Add ExecutionContextJob to interop execution style with Job style: https://github.com/twitter/scalding/pull/952

### Version 0.11.2 ###
* hadoop.tmp.dir for snapshot in config

### Version 0.11.1 ###
* Fixes bad release portion where code wasn't updated for new scalding version number.
* use cascading-jdbc 2.5.3 for table exists fix and cascading 2.5.5: https://github.com/twitter/scalding/pull/951
* Bump build properties and sbt launcher: https://github.com/twitter/scalding/pull/950
* Fixes the travis build: https://github.com/twitter/scalding/pull/944
* Making the README.md consistent with 0.11.0 changes for the REPL.: https://github.com/twitter/scalding/pull/941
* Backport Meatlocker: https://github.com/twitter/scalding/pull/571

### Version 0.11.0 ###
* REPL: Add toIterator (and related methods): https://github.com/twitter/scalding/pull/929
* Fix the build to use the shared module method: https://github.com/twitter/scalding/pull/938
* Clean up the UniqueID stuff, to avoid plumbing it everywhere: https://github.com/twitter/scalding/pull/937
* TypedPipe.from(List).distinct fails: https://github.com/twitter/scalding/pull/935
* Clean up ExecutionContext a bit: https://github.com/twitter/scalding/pull/933
* Fix Issue 932: no-op Jobs should not throw: https://github.com/twitter/scalding/pull/934
* Use Execution to run flows in REPL: https://github.com/twitter/scalding/pull/928
* Snapshot a pipe in the REPL: https://github.com/twitter/scalding/pull/918
* Add support for AppJar in Config: https://github.com/twitter/scalding/pull/924
* Fix LzoTextLine as a TypedSource: https://github.com/twitter/scalding/pull/921
* Use externalizer in BijectedSourceSink: https://github.com/twitter/scalding/pull/926
* Add an Executor to run flows without a Job: https://github.com/twitter/scalding/pull/915
* This handles the case where scalding will save out a tsv and re-use it down stream leading to issues where the types are not strings: https://github.com/twitter/scalding/pull/913
* Fix DailySuffixTsv for testability, remove leaked DailySuffixTsv: https://github.com/twitter/scalding/pull/919
* Add a Config class to make configuration understandable: https://github.com/twitter/scalding/pull/914
* Integrate the repl completely into scald.rb. Fixup scald-rb for better hdfs-local mode now with our provides: https://github.com/twitter/scalding/pull/902
* Add some auto-reformats: https://github.com/twitter/scalding/pull/911
* Update JDBCSource: https://github.com/twitter/scalding/pull/898
* Allow tests for typed delimited by fixing swallowed bug: https://github.com/twitter/scalding/pull/910
* Add Hadoop platform test to enable unit testing for Hadoop semantics: https://github.com/twitter/scalding/pull/858
* Some minor improvements to typed joining code: https://github.com/twitter/scalding/pull/909
* Fix #906: https://github.com/twitter/scalding/pull/908
* Run the test target, so the tests are reformatted: https://github.com/twitter/scalding/pull/907
* Enable scalariform: https://github.com/twitter/scalding/pull/905
* Simplify &quot;scald-repl.sh&quot;: https://github.com/twitter/scalding/pull/901
* Typed Tutorial: https://github.com/twitter/scalding/pull/897
* Adding a test for the scalding repl: https://github.com/twitter/scalding/pull/890
* Properly close tuple iterator in test framework.: https://github.com/twitter/scalding/pull/896
* Add constructors to ValuePipe: https://github.com/twitter/scalding/pull/893
* contraMap and andThen on TypedSink/TypedSource: https://github.com/twitter/scalding/pull/892
* Tiny fix to use an ImplicitBijection rather than Bijection: https://github.com/twitter/scalding/pull/887
* Feature/bijected source sink: https://github.com/twitter/scalding/pull/886
* Fix intersection equality error: https://github.com/twitter/scalding/pull/878
* Add DailySuffixTypedTsv and HourlySuffixTypedTsv.: https://github.com/twitter/scalding/pull/873
* add stepListner register support in Scalding: https://github.com/twitter/scalding/pull/875
* Backport Meatlocker: https://github.com/twitter/scalding/pull/571

### Version 0.10.0 ###
* Upgrade cascading to 2.5.4, cascading jdbc to 2.5.2
* Adding an hdfs mode for the Scalding REPL
* Added implementation of PartitionSource with tests
* Add helper methods to KeyedList and TypedPipe
* Add addTrap to TypedPipe

### Version 0.9.0 ###
* Add join operations to TypedPipe that do not require grouping beforehand
* Fixed bug in size estimation of diagonal matrices
* Optimized the reduceRow/ColVectors function for the number of reducers
* Add a BlockMatrix object (an abstraction of a Vector of Matrices)

### Version 0.8.8 ###
* Publish 0.8.7 for scala 2.9.3.

### Version 0.8.7 ###
* Hotfix to bypass a bug in Hadoop, which cannot sync up all deprecated keys.

### Version 0.8.6 ###
* Hotfix a bug in Tool. Now Tool will re-throw all exceptions again.

### Version 0.8.5 ###
* Fixed bug in RichPipe.insert
* Add souce[T] to JobTest
* Allow DelimitedScheme to override strictness and safety.
* Add distinct method to RichPipe and TypedPipe, add mapValues to TypedPipe
* ISSUE 389: Catch exceptions in Tool
* Sbt assembly 0.8.7
* Add CascadeJob to allow multiple flows in one job
* Adding cross-build scala versions
* Use mima to check binary compatibility

### Version 0.8.4 ###
* ISSUE 340: Upgrade to Cascading 2.1.5
* ISSUE 327,329,337: adds sample method with seed in RichPipe
* ISSUE 323: Remove untyped write from TypedPipe (must write to Mappable[U])
* ISSUE 321: pulls out scalding-date and scalding-args as separate projects

#### Contributions ####
27 commits
* P. Oscar Boykin: 13 commits
* willf: 4 commits
* Argyris Zymnis: 3 commits
* Sam Ritchie: 3 commits
* Tim Chklovski: 1 commits
* Chris Severs: 1 commits
* Rickey Visinski: 1 commits
* David Shimon: 1 commits

### Version 0.8.3 ###
* ISSUE 312: dramatic speedup for sortWithTake/sortedTake if you take many items
* ISSUE 307: Read support for JsonLine support (previously, just write)
* ISSUE 305: Adds a shuffle-method to RichPipe (for sampling/sharding)
* ISSUE 299: limit method for Typed-safe API.
* ISSUE 296: Fixes self-joins in the Type-safe API.
* ISSUE 295: unpack-all syntax (use Fields.ALL) for TupleUnpacker
* ISSUE 280: Improvements to AbsoluteDuration
* ISSUE 277: Upgrade to Cascading 2.0.7

#### Contributions ####
75 commits total.

* P. Oscar Boykin: 25 commits
* Alex Dean: 15 commits
* Argyris Zymnis: 11 commits
* Sam Ritchie: 4 commits
* Timothy Chklovski: 4 commits
* Dan McKinley: 4 commits
* Aaron Siegel: 3 commits
* Tim Chklovski: 3 commits
* Ashutosh Singhal: 2 commits
* João Oliveirinha: 2 commits
* Arkajit Dey: 1 commits
* Avi Bryant: 1 commits

### Version 0.8.2 ###
* ISSUE 269: Improvements to AbsoluteDuration.fromMillisecs and some new APIs
* ISSUE 256: Weighted page-rank with the Matrix API
* ISSUE 249: Fix for Matrix.scala missing some obvious operations
* ISSUE 246: Partition in RichPipe (create a new field, and then groupBy on it)
* ISSUE 241: Fix joinWithLarger with a custom joiner
* ISSUE 234. 238: Etsy sync: periodic date jobs, ability to add traps, more flexible Args
* ISSUE 230: shard and groupRandomly on RichPipe
* ISSUE 229: Initial skew-Join implementation (please test!)
* ISSUE 228 - 233, 265: Improve Typed-API
* ISSUE 221: Combinatorics in scalding.mathematics

#### Contributions ####
106 commits total.

* P. Oscar Boykin: 29 commits
* Krishnan Raman: 16 commits
* Arkajit Dey: 15 commits
* Avi Bryant: 7 commits
* Edwin Chen: 6 commits
* Aaron Siegel: 5 commits
* Koert Kuipers: 5 commits
* Argyris Zymnis: 4 commits
* Sam Ritchie: 4 commits
* Chris Severs: 4 commits
* Brad Greenlee: 3 commits
* Wil Stuckey: 2 commits
* Dan McKinley: 2 commits
* Matteus Klich: 2 commits
* Josh Devins: 1 commits
* Steve Mardenfeld: 1 commits

### Version 0.8.1 ###
* ISSUE 220: Etsy date improvements and local-mode tap improvements
* ISSUE 219: scald.rb fix
* ISSUE 218: Add aggregate method to ReduceOperations
* ISSUE 216: Improve variance notations
* ISSUE 213,215: Make Field[T] serializable
* ISSUE 210,211: Refactor date code into individual files + tests
* ISSUE 209: Add hourly/daily time pathed source classes
* ISSUE 207,208: sbt build improvements
* ISSUE 205,206: Remove scala serialization code to com.twitter.chill
* ISSUE 203: Improved date-parsing and docs (from Etsy)
* ISSUE 202,204: Add propagate/mapWithIndex in Matrix (use Monoids with graphs)
* ISSUE 201: add stdDev to groupBuilder.
* ISSUE 200: typed write and key-value swap
* ISSUE 196: Clean up deprecations
* ISSUE 194,195: Fix negative numbers as args
* ISSUE 190-192,197,199: Improved Joining/Co-grouping in the typed-API

### Version 0.8.0 ###
* Many small bugfixes
* ISSUE 189: Adds spillThreshold to GroupBuilder to tune memory usage
* ISSUE 187: Adds TypedTsv for type-safe TSV files.
* ISSUE 179: Add forceToDisk to help hand optimization of flows
* ISSUE 175: API to set the type/comparators in the Fields API.
* ISSUE 162, 163: Adds keys, values methods to Typed-API
* ISSUE 160: adds approxUniques to GroupBuilder
* ISSUE 153: mapPlusMap in GroupBuilder
* ISSUE 149: Support for Hadoop sequence files
* ISSUE 148: Matrix API
* ISSUE 147: Move Monoid/Algebra code to Algebird
* Mulitple issues: many new Kryo serializers added
* ISSUE 140: Adds ability to do side-effects (foreach, using in RichPipe)

### Version 0.7.3 ###
* ISSUE 134: Cleans up scalding.Tool
* ISSUE 133: Adds SortedListTake monoid, Either monoid, and tests
* ISSUE 130: Upgrades cascading.kryo to 0.4.4
* ISSUE 129: Disable Kryo references by default, add config
* ISSUE 128: Minor fix to normalize job

### Version 0.7.2 ###
* ISSUE 127: Upgrade maple and remove jdks.
* ISSUE 126: Switch normalize to use crossWithTiny
* ISSUE 125: Serialization fixes

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
