(ns cascalog.ops
  (:refer-clojure :exclude [count min max comp juxt partial])
  (:use cascalog.api
        [jackknife.def :only (defnk)]
        [jackknife.seq :only (collectify)]
        [cascalog.workflow :only (fill-tap!)]
        [cascalog.io :only (with-fs-tmp)])
  (:require [cascalog.util :as u]
            [cascalog.ops-impl :as impl]
            [cascalog.vars :as v]))

;; Operation composition functions

(defn negate
  "Accepts a filtering op and returns an new op that acts as the
  negation (or complement) of the original. For example:

  ((negate #'string?) ?string-var) ;; within some query

  Is equivalent to

  ;; within some query
  (string? ?string-var :> ?temp-bool)
  (not ?temp-bool)"
  [op]
  (<- [:<< !invars :> !true?]
      (op :<< !invars :> !curr?)
      (not !curr? :> !true?)))

(defn all
  "Accepts any number of filtering ops and returns a new op that
  checks that every every one of the original filters passes. For
  example:

  ((all #'even? #'positive? #'small?) ?x) ;; within some query

  Is equivalent to:

  ;; within some query
  (even? ?x :> ?temp1)
  (positive? ?x :> ?temp2)
  (small? ?x) :> ?temp3)
  (and ?temp1 ?temp2 ?temp3)"
  [& ops]
  (impl/logical-comp ops #'impl/bool-and))

(defn any
  "Accepts any number of filtering ops and returns a new op that
  checks that at least one of the original filters passes. For
  example:

  ((any #'even? #'positive? #'small?) ?x) ;; within some query

  Is equivalent to:

  ;; within some query
  (even? ?x :> ?temp1)
  (positive? ?x :> ?temp2)
  (small? ?x :> ?temp3)
  (or ?temp1 ?temp2 ?temp3)"
  [& ops]
  (impl/logical-comp ops #'impl/bool-or))

(defn comp
  "Accepts any number of predicate ops and returns an op that is the
  composition of those ops.

  (require '[cascalog.ops :as c])
  ((c/comp #'str #'+) ?x ?y :> ?sum-string) ;; within some query

  Is equivalent to:

  ;; within some query
  (+ ?x ?y :> ?intermediate)
  (str ?intermediate :> ?sum-string)"
  [& ops]
  (let [[invar & more] (v/gen-nullable-vars (inc (clojure.core/count ops)))
        allvars (list* invar (map vector more))]
    (construct [:<< invar :> (last more)]
               (map (fn [o [invars outvars]]
                      [o :<< invars :>> outvars])
                    (reverse ops)
                    (partition 2 1 allvars)))))

(defn juxt
  "Accepts any number of predicate ops and returns an op that is the
  juxtaposition of those ops.

  (require '[cascalog.ops :as c])
  ((c/juxt #'+ #'- #'<) !x !y :> !sum !diff !mult) ;; within some query

  Is equivalent to:

  ;; within some query
  (+ !x !y :> !sum)
  (- !x !y :> !diff)
  (* !x !y :> !mult)"
  [& ops]
  (let [outvars (v/gen-nullable-vars (clojure.core/count ops))]
    (construct
     [:<< "!invars" :>> outvars]
     (map (fn [o v] [o :<< "!invars" :> v])
          ops
          outvars))))

(defn each
  "Accepts an operation and returns a predicate macro that maps `op`
  across any number of input variables. For example:

  ((each #'str) ?x ?y ?z :> ?x-str ?y-str ?z-str) ;; within some query

  Is equivalent to

  ;; within some query
  (str ?x :> ?x-str)
  (str ?y :> ?y-str)
  (str ?z :> ?z-str)"
  [op]
  (predmacro [invars outvars]
             {:pre [(or (zero? (clojure.core/count outvars))
                        (= (clojure.core/count invars)
                           (clojure.core/count outvars)))]}
             (if (empty? outvars)
               (for [i invars]
                 [op i])
               (map (fn [i v] [op i :> v])
                    invars
                    outvars))))

(defn partial
  "Accepts an operation and fewer than normal arguments, and returns a
  new operation that can be called with the remaining unspecified
  args. For example, given this require and defmapop:

  (require '[cascalog.ops :as c])
  (defmapop plus [x y] (+ x y))

  The following two forms are equivalent: 

  (let [plus-10 (c/partial plus 10)]
     (<- [?y] (src ?x) (plus-10 ?x :> ?y)))

  (<- [?y] (src ?x) (plus-10 ?x :> ?y))

  With the benefit that `10` doesn't need to be hardcoded into the
  first query."
  [op & args]
  (predmacro
   [invars outvars]
   [[op :<< (concat args invars) :>> outvars]]))

;; Operations to use within queries

(defmapop [re-parse [pattern]]
  "Accepts a regex `pattern` and a string argument `str` and returns
  the groups within `str` that match the supplied `pattern`."
  [str]
  (re-seq pattern str))

(defparallelagg count
  :init-var #'impl/one
  :combine-var #'+)

(def sum (each impl/sum-parallel))

(def min (each impl/min-parallel))

(def max (each impl/max-parallel))

(def !count (each impl/!count-parallel))

(defparallelbuf limit
  :hof? true
  :init-hof-var #'impl/limit-init
  :combine-hof-var #'impl/limit-combine
  :extract-hof-var #'impl/limit-extract
  :num-intermediate-vars-fn (fn [infields outfields]
                              (clojure.core/count infields))
  :buffer-hof-var #'impl/limit-buffer)

(def limit-rank
  (merge limit {:buffer-hof-var #'impl/limit-rank-buffer}))

(def ^{:doc "Predicate operation that produces the average value of the
  supplied input variable. For example:

  (let [src [[1] [2]]]
    (<- [?avg]
        (src ?x)
        (avg ?x :> ?avg)))
  ;;=> ([1.5])"}
  avg
  (<- [!v :> !avg]
      (count !c)
      (sum !v :> !s)
      (div !s !c :> !avg)))

(def ^{:doc "Predicate operation that produces a count of all distinct
  values of the supplied input variable. For example:

  (let [src [[1] [2] [2]]]
  (<- [?count]
      (src ?x)
      (distinct-count ?x :> ?count)))
  ;;=> ([2])"}
  distinct-count
  (<- [:<< !invars :> !c]
      (:sort :<< !invars)
      (impl/distinct-count-agg :<< !invars :> !c)))
      
(defn fixed-sample-agg [amt]
  (<- [:<< ?invars :>> ?outvars]
      ((cascalog.ops.RandLong.) :<< ?invars :> ?rand)
      (:sort ?rand)
      (limit [amt] :<< ?invars :>> ?outvars)))

;; Common patterns

(defn lazy-generator
  "Returns a cascalog generator on the supplied sequence of
  tuples. `lazy-generator` serializes each item in the lazy sequence
  into a sequencefile located at the supplied temporary directory and returns
  a tap for the data in that directory.

  It's recommended to wrap queries that use this tap with
  `cascalog.io/with-fs-tmp`; for example,

    (with-fs-tmp [_ tmp-dir]
      (let [lazy-tap (lazy-generator tmp-dir lazy-seq)]
        (?<- (stdout)
             [?field1 ?field2 ... etc]
             (lazy-tap ?field1 ?field2)
             ...)))"
  [tmp-path [tuple :as l-seq]]
  {:pre [(coll? tuple)]}
  (let [tap (:sink (hfs-seqfile tmp-path))
        n-fields (clojure.core/count tuple)]
    (fill-tap! tap l-seq)
    (name-vars tap (v/gen-nullable-vars n-fields))))

(defnk first-n
  "Accepts a generator and a number `n` and returns a subquery that
   produces the first n elements from the supplied generator. Two
   boolean keyword arguments are supported:

  :sort -- accepts a vector of variables on which to sort. Defaults to
           nil (unsorted).
  :reverse -- If true, sorts items in reverse order. (false by default).

  For example:
 
  (def src [[1] [3] [2]]) ;; produces 3 tuples

  ;; produces ([1 2] [3 4] [2 3]) when executed
  (def query (<- [?x ?y] (src ?x) (inc ?x :> ?y)))

  ;; produces ([3 4]) when executed
  (first-n query 1 :sort [\"?x\"] :reverse true)"
  [gen n :sort nil :reverse false]
  (let [num-fields (num-out-fields gen)
        in-vars  (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)
        sort-set (if sort (-> sort collectify set) #{})
        sort-vars (if sort
                    (mapcat (fn [f v] (if (sort-set f) [v]))
                            (get-out-fields gen)
                            in-vars))]
    (<- out-vars
        (gen :>> in-vars)
        (:sort :<< sort-vars)
        (:reverse reverse)
        (limit [n] :<< in-vars :>> out-vars))))

(defn fixed-sample
  "Returns a subquery getting a random sample of n elements from the generator"
  [gen n]
  (let [num-fields (num-out-fields gen)
        in-vars  (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)]
    (<- out-vars
        (gen :>> in-vars)
        ((fixed-sample-agg n) :<< in-vars :>> out-vars))))

;; Helpers to use within ops

(defmacro with-timeout
  "Accepts a vector with a timeout (in ms) and any number of forms and
  executes those forms sequentially. returns the result of the last
  form or nil (if the timeout is reached.) For example:

  (with-timeout [100]
    (Thread/sleep 50)
    \"done!\")
  ;;=> \"done!\"

  (with-timeout [100]
    (Thread/sleep 200)
    \"done!\")
  ;;=> nil"
  [[ms] & body]
  `(let [^java.util.concurrent.Future f# (future ~@body)]
     (try (.get f# ~ms java.util.concurrent.TimeUnit/MILLISECONDS)
          (catch java.util.concurrent.TimeoutException e#
            (.cancel f# true)
            nil))))
