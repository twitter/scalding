(ns cascalog.predicate-test
  (:use cascalog.predicate
        clojure.test
        midje.sweet
        cascalog.testing)
  (:require [cascalog.workflow :as w]))

(w/defmapop timesplusone ["blahfield"]
  [a b]
  (inc (* a b)))

(deftest test-map-pred
  (let [pred (build-predicate {}
                              timesplusone
                              nil
                              ["?f1" "?f2"]
                              ["?q"])
        source-data {:fields ["?a" "?b" "?f1" "?f2" "?c"]
                     :tuples [[1 2 1 1 10]
                              [0 0 2 6 9]
                              [0 0 9 1 0]]}
        sink-data   {:fields ["?q"]
                     :tuples [[2] [13] [10]]}]
    (fact "Check that predicate contains proper map entries."
      pred => (contains {:type      :operation
                         :infields  ["?f1" "?f2"]
                         :outfields ["?q"]}))
    (test-assembly source-data sink-data (:assembly pred))))

(w/defmapop addplusone ["blah" "blah2"]
  [& all]
  [(inc (apply + all))
   (first all)])

(deftest test-variable-substitution
  (let [pred (build-predicate {}
                              addplusone
                              nil
                              ["?f1" "?f2" 3 4 "?f3"]
                              ["?s" "?s2"])
        source-data {:fields ["?f1" "?f2" "?f3"]
                     :tuples [[6 2 3]
                              [8 12 19]
                              [6 7 12]
                              [1 4 8]]}
        sink-data   {:fields ["?s" "?s2"]
                     :tuples [[33 6] [19 6] [47 8] [21 1]]}]
    (fact "Check that all variable-sub properties are satisfied."
      pred => (contains {:type      :operation
                         :infields  ["?f1" "?f2" "?f3"]})
      (:outfields pred) => (contains ["?s" "?s2"] :in-any-order :gaps-ok)
      (:outfields pred) => #(= 4 (count %)))
    (test-assembly source-data sink-data (:assembly pred))))

(w/defmapop nilop ["f1" "f2"] [a]
  (if (= a 1)
    [nil a]
    [a nil]))

(deftest test-nil-filtering
  (let [pred (build-predicate
              {}
              nilop
              nil
              ["?i"]
              ["?o1" "!o2"])
        source-data {:fields ["?i"]
                     :tuples [[1] [2] [3] [1]]}
        sink-data   {:fields ["?o1" "!o2"]
                     :tuples [[2 nil] [3 nil]]} ]
    (test-assembly source-data sink-data (:assembly pred))))

(w/defmapcatop many-vals ["val"] [n]
  (cond (odd? n) [(* n 2) (* 3 n) (* n n)]
        (= n 2)  []
        :else     [(inc n)]))

(deftest test-mapcat-pred
  (let [pred (build-predicate {} many-vals nil ["?a"] ["?b"])
        source-data {:fields ["?a"]
                     :tuples [[1] [2] [3] [4]]}
        sink-data   {:fields ["?b"]
                     :tuples [[2] [3] [1] [6] [9] [9] [5]]} ]
    (test-assembly source-data sink-data (:assembly pred))))

(future-fact "Test filter predicate.")

(deftest test-vanilla-filter
  (let [pred (build-predicate {} odd? nil ["?f"] [])
        source-data {:fields ["?f"]
                     :tuples [[1] [2] [3] [4] [6] [9] [10]]}
        sink-data   {:fields ["?f"]
                     :tuples [[1] [3] [9]]} ]
    (test-assembly source-data sink-data (:assembly pred))))

(deftest test-filter-func
  (let [pred (build-predicate {} odd? nil ["?f"] ["?o"])
        source-data {:fields ["?f"]
                     :tuples [[1] [2] [3] [4] [6] [9] [10]]}
        sink-data   {:fields ["?f" "?o"]
                     :tuples [[1 true] [2 false] [3 true]
                              [4 false] [6 false] [9 true]
                              [10 false]]} ]
    (test-assembly source-data sink-data (:assembly pred))))

(future-fact "Test generator.")
