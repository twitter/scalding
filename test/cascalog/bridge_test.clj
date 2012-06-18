(ns cascalog.bridge-test
  (:use midje.sweet)
  (:require [cascalog.workflow :as w]
            [cascalog.testing :as t]
            [cascalog.util :as u])
  (:import [cascading.tuple Fields]
           [cascading.pipe Pipe]
           [cascalog ClojureFilter ClojureMap ClojureMapcat
            ClojureAggregator Util]))

(fact "ns-fn-name-pair should produce a pair of strings."
  (w/ns-fn-name-pair #'str) => ["clojure.core" "str"])

(def obj-array-class
  (Class/forName "[Ljava.lang.Object;"))

(defn plus-one [in]
  [(+ in 1)])

(defn plus-n [n]
  (fn [in]
    [(+ in n)]))

(defn inc-wrapped [num]
  [(inc num)])

(defn inc-both [num1 num2]
  [(inc num1) (inc num2)])

(defn is-type
  "Accepts a class and returns a checker that tests whether or not its
  input is an instance of the supplied class."
  [^Class expected]
  (chatty-checker
   [actual]
   (instance? expected actual)))

(tabular
 (fact
   "fn-spec should propery resolve a var (or vector of var and
   arguments) into an object array of namespace, function name
   and (optionally) arguments."
   (let [spec (w/fn-spec ?input)]
     spec       => (is-type obj-array-class)
     (seq spec) => ?result-vec))
 ?input       ?result-vec
 #'plus-one   ["cascalog.bridge-test" "plus-one"]
 [#'plus-n 3] ["cascalog.bridge-test" "plus-n" 3])


(tabular
 (fact "bootFn tests, simple and higher order."
   (let [spec (into-array Object ?spec)
         f    (Util/bootFn spec)]
     (f 1) => ?result))
 ?spec                               ?result
 ["cascalog.bridge-test" "plus-one"] [2]
 ["cascalog.bridge-test" "plus-n" 3] [4])

(facts "Fields tests."
  (let [f1 (w/fields "foo")
        f2 (w/fields ["foo" "bar"])]
    (facts "Single fields should resolve properly."
      f1       => #(instance? Fields %)
      (seq f1) => ["foo"])

    (facts "Double fields should resolve properly."
      f2       => #(instance? Fields %)
      (seq f2) => ["foo" "bar"])))

(tabular 
 (fact "Pipe testing."
   ?pipe => #(instance? Pipe %)
   (.getName ?pipe) => ?check)
 ?pipe           ?check
 (w/pipe)        #(= 36 (.length %))
 (w/pipe "name") "name")

(fact "Clojure Filter test."
  (let [fil (ClojureFilter. (w/fn-spec #'odd?) false)]
    (t/invoke-filter fil [1]) => false
    (t/invoke-filter fil [2]) => true))

(tabular
 (fact "ClojureMap test, single field."
   (t/invoke-function ?clj-map [1]) => [[2]])
 ?clj-map
 (ClojureMap. (w/fields "num")
              (w/fn-spec #'inc-wrapped)
              false)
 (ClojureMap. (w/fields "num")
              (w/fn-spec #'inc)
              false))

(facts "ClojureMap test, multiple fields."
  (let [m (ClojureMap. (w/fields ["num1" "num2"])
                       (w/fn-spec #'inc-both)
                       false)]
    (t/invoke-function m [1 2]) => [[2 3]]))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)]
        [(+ num 2)]
        [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1)
        (+ num 2)
        (+ num 3)))

(tabular
 (fact "ClojureMapCat test, single field."
   (t/invoke-function ?clj-mapcat [1]) => [[2] [3] [4]])
 ?clj-mapcat
 (ClojureMapcat. (w/fields "num")
                 (w/fn-spec #'iterate-inc-wrapped)
                 false)
 (ClojureMapcat. (w/fields "num")
                 (w/fn-spec #'iterate-inc)
                 false))

(defn sum
  ([] 0)
  ([mem v] (+ mem v))
  ([mem] [mem]))

(fact "ClojureAggregator test."
  (let [a (ClojureAggregator. (w/fields "sum")
                              (w/fn-spec #'sum)
                              false)]
    (t/invoke-aggregator a [[1] [2] [3]]) => [[6]]))
