### 1.9.0

* Cascalog now uses the Apache 2.0 license (formerly used GNU)
* Support for Cascading 2.0 (Hadoop planner only, for now)
* Cascalog traps no longer trap the entire failing tuple; just the specific fields that caused the failure.
* Sinkmode options are now :keep, :update and :replace, vs the old :keep, :append and :replace.
* Extensive documentation of functions in cascalog.ops
* Cascalog now pulls in Clojure 1.4.0 by default (with testing against 1.2 and 1.3)
* `cascalog.StdoutTap` moved to `com.twitter.maple.StdoutTap`
* `:distinct` defaults to false instead of true (!!)

## 1.8.7

* Exposed a pure-Java, fully-interoperable API to Cascalog called JCascalog
* Upgraded to Kryo 2
* Functions no longer need to be passed around as vars to be used in queries (although they still need to be bound to vars somewhere).
* Fixed bug where KryoService would fail when two jobs were called simultaneously from different threads (as with cascalog.checkpoint).
* Added fixed-sample query builder function for getting a fixed-cardinality uniform random sample of a generator. The implementation is efficient and can be used on any size dataset.
* Added fixed-sample-agg aggregator to ops for taking a fixed-cardinality sample of the group.
* Regular Java lists can now be used as generators
* cascalog.playground/bootstrap* functions will now lower io.sort.mb to 1 for all jobs so they run faster locally.
* Bug fix: lazy-generator no longer filters out tuples with nil values.
* Bug fix: make vars containing predmacros work properly

## 1.8.6

* "Call to unbound-fn" Error solved. To use functions within Cascalog queries, hadoop needed to call "require" on the containing namespace. If you define functions at the repl, the namespace file might not exist, and this call will fail; previously, Cascalog would squash these exceptions.

This fix explicitly checks namespace existence, letting all others errors pass through. Anyone seeing an "unbound fn" exception will now see some far more enlightening exception about what's wrong with the namespace.

* Anything that implements IFn can now be used as an op, provided it's bound to a var. For example, `(def is-bob? #{"bob"})` is now a valid predicate.

* Vars can now be serialized as op parameters or constants. For example,

```clojure
;; Vars as parameter args
(defmapop var-apply [v]
   [& xs]
   (apply v xs))

(fact?<- [[1 2 3]]
         [?x ?y ?z]
         ([[1 2]] ?x ?y)
         (var-apply [#'+] ?x ?y :> ?z))

;; Vars as constants
(def coll-src
    [[[3 2 4 1]]
     [[1 2 3 4 5]]])

(fact?<- [[10] [15]]
         [?sum]
         (coll-src ?coll)
         (reduce #'+ ?coll :> ?sum))
```

* Added explicit Kryo serialization for `java.util.regex.Pattern`.
* Kryo serialization now captures objects without default constructors.
* MemorySourceTap (and data structures) now use Hadoop's serialization mechanisms, vs defaulting to Kryo.

## 1.8.5

* Memory-Source-Tap now uses project settings from job-conf.clj (bugfix!)
* Kryo Serialization for clojure primitives, clojure collections and select java primitives.
* Functions defined with defmain are now callable by their supplied names. (Previously they weren't usable except to generate aot-compiled classes.)
* `bootstrap` and `bootstrap-emacs` are now functions, not macros.
* defops and defmain include :skip-wiki true in metadata, allowing them to be used with codox or autodoc and not generate clutter.
* Sets are no longer destructured as tuples by Cascalog.

In addition to previous support for the various number types + strings, the following classes are now serialized by default:

- **Java primitives & Collections**
  - java.math.BigDecimal
  - java.math.BigInteger
  - java.util.Date
  - java.util.UUID
  - java.sql.Date
  - java.sql.Time
  - java.sql.Timestamp
  - java.net.URI
  - java.util.ArrayList
  - java.util.HashMap
  - java.util.HashSet

- **Clojure Primitives**
  - clojure.lang.Keyword
  - clojure.lang.Symbol
  - clojure.lang.Ratio
  - clojure.lang.Var
  - clojure.lang.BigInt (only in Clojure 1.3)

- **Clojure Collections and sequences:**
  - clojure.lang.Cons
  - clojure.lang.IteratorSeq
  - clojure.lang.LazySeq
  - clojure.lang.ArraySeq
  - clojure.lang.MapEntry
  - clojure.lang.PersistentArrayMap
  - clojure.lang.PersistentHashMap
  - clojure.lang.PersistentHashSet
  - clojure.lang.PersistentList
  - clojure.lang.PersistentList$EmptyList
  - clojure.lang.PersistentStructMap
  - clojure.lang.PersistentVector
  - clojure.lang.StringSeq

## 1.8.4

* Add project-wide jobconf settings in job-conf.clj.
* optional docstring and metadata support on defparallelagg and defparallelbuf.
* def*ops now present proper argument list metadata.
* Added cascalog.ops/partial
* fixed :sinkmode docstring.
* defaggregateop no longer fails when returning nil
* Better error reporting on parametrized ops (variadic args aren't supported; this now throws an error.)
* MUCH better error messaging on failed tests. Here's the old way:

    expected: (= (map multi-set (map doublify set1)) (map multi-set (map doublify set2)))
      actual: (not (= ({[2.0] 1}) ({[1.0] 1})))

vs the new:

    expected: (= input output)
    actual: (not (= [[2.0]] [[1.0]]))
