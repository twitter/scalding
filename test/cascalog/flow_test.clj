(ns cascalog.flow-test
  (:use clojure.test
        cascalog.testing)
  (:import [cascading.tuple Fields])
  (:require [cascalog.workflow :as w]))

(deftest test-simple-assembly
  (let [source-data {:fields ["a" "b"] :tuples [[2 1] [11 10]]}
        sink-data {:fields ["b" "c"] :tuples [[1 3] [10 12]]}]
    (test-assembly source-data sink-data
                   (w/map #'inc "a" :fn> "c" :> ["b" "c"]))))

(w/defmapop add-double
  [v1 v2]
  (* 2 (+ v1 v2)))

(w/defmapop stateful-add
  {:stateful true}
  ([] 10)
  ([state val] (+ state val))
  ([state] nil))

(deftest test-statefulmapop
  (let [source-data {:fields ["n"]
                     :tuples [[1] [2] [3]]}
        sink-data {:fields ["v"]
                   :tuples [[11] [12] [13]]}]
    (test-assembly source-data sink-data
                   (stateful-add "n" :fn> "v" :> "v"))))

(deftest test-map-op
  (let [source-data {:fields ["n1" "n2"]
                     :tuples [[1 0] [2 -3] [5 7]]}
        sink-data {:fields ["v"]
                   :tuples [[2] [-2] [24]]}]
    (test-assembly source-data sink-data
                   (add-double ["n1" "n2"] :fn> "v" :> "v"))))

(w/defmapcatop keeper-dropper ["a"] [v]
  (cond (= v 1)  [v]
        (odd? v) []
        :else    [v (inc v)]))

(deftest test-mapcat-op
  (let [source-data {:fields ["n"]
                     :tuples [[1] [2] [3] [4] [9]]}
        sink-data {:fields ["a"]
                   :tuples [[1] [2] [3] [4] [5]]}]
    (test-assembly source-data sink-data
                   (keeper-dropper "n" :> "a"))))

(deftest test-filter-filter)

(deftest test-filter-map)

(deftest test-join)

(deftest test-higher-order-op)

(w/defbufferop emit-odd ["e"] [vals]
  (filter (comp odd? first) vals))

(deftest test-buffer
  (let [source-data {:fields ["f1" "f2"]
                     :tuples [["a" 1] ["a" 2] ["b" 3]
                              ["a" 4] ["b" 8] ["a" 7]
                              ["c" 7] ["d" 6]]}
        sink-data {:fields ["f1" "q"]
                   :tuples [["a" 1] ["a" 7]
                            ["b" 3] ["c" 7]]}]
    (test-assembly source-data sink-data
                   (w/assembly [p] (p (w/group-by "f1")
                                      (emit-odd "f2" :fn> "q"))))))

(w/defaggregateop sum-even-2out
  ([] 0)
  ([v n] (if (odd? n) v (+ v n)))
  ([v] [v (inc v)]))

(deftest test-aggregator
  (let [source-data {:fields ["f1" "f2"]
                     :tuples [["a" 1] ["a" 2] ["b" 3]
                              ["a" 4] ["b" 9]
                              ["a" 7] ["c" 8]]}
        sink-data {:fields ["f1" "t"]
                   :tuples [["a" 6] ["a" 7]
                            ["b" 0] ["b" 1]
                            ["c" 8] ["c" 9]]}]
    (test-assembly source-data sink-data
                   (w/assembly [p] (p (w/group-by "f1")
                                      (sum-even-2out "f2" :fn> "t"))))))

;; need to rename pipes coming from same source

(deftest self-join-test
  (let [source-data {:fields ["a" "b"]
                     :tuples [["a" 1] ["b" 2]
                              ["a" 3] ["c" 4]]}
        sink-data   {:fields ["a" "s" "c"]
                     :tuples [["a" 4.0 2] ["a" 4.0 4]
                              ["b" 2.0 3] ["c" 4.0 5]]}
        assembly    (w/assembly [p] [p1 (p (w/group-by "a")
                                           (w/sum "b" "s")
                                           (w/pipe-name "1"))
                                     p2 (p (w/map #'inc "b" :fn> "c" :> ["a" "c"])
                                           (w/pipe-name "2"))]
                                ([p1 p2] (w/inner-join ["a" "a"] ["a" "s" "a2" "c"])
                                   (w/select ["a" "s" "c"])))]
    (test-assembly source-data sink-data assembly)))

(deftest disconnected-test
  (let [source-data [{:fields ["a"]
                      :tuples [[1] [2] [3]]}
                     {:fields ["b"]
                      :tuples [[10]]}]
        sink-data   [{:fields ["x"]
                      :tuples [[2] [3] [4]]}
                     {:fields ["y"]
                      :tuples [[9]]}]
        assembly    (w/assembly [p1 p2] [p1 (p1 (w/map #'inc "a" :fn> "x" :> "x"))
                                         p2 (p2 (w/map #'dec "b" :fn> "y" :> "y"))]
                                [p1 p2])]
    (test-assembly source-data sink-data assembly)))

(deftest self-only-join-test
  (let [source-data {:fields ["k" "v"]
                     :tuples [["a" 1] ["b" 2] ["a" 3]]}
        sink-data   {:fields ["k" "v" "v2"]
                     :tuples [["a" 1 1] ["a" 3 1]
                              ["a" 1 3] ["a" 3 3]
                              ["b" 2 2]]}
        assembly   (w/assembly [p]
                               ([p p] (w/inner-join ["k" "k"] ["k" "v" "k2" "v2"])
                                  (w/select ["k" "v" "v2"])))]
    (test-assembly source-data sink-data assembly)))

(deftest self-join-after-agg-test
  (let [source-data {:fields ["a" "b"]
                     :tuples [["a" 1] ["b" 2] ["a" 3] ["c" 4]]}
        sink-data   {:fields ["a" "s" "z"]
                     :tuples [["a" 4.0 5.0] ["b" 2.0 3.0] ["c" 4.0 5.0]]}
        assembly    (w/assembly [p] [p (p (w/pipe-name "1")
                                          (w/group-by "a")
                                          (w/sum "b" "s"))
                                     p2 (p (w/pipe-name "2")
                                           (w/map #'inc "s" :fn> "z" :> ["a" "z"]))]
                                ([p p2] (w/inner-join ["a" "a"] ["a" "s" "a2" "z"])
                                   (w/select ["a" "s" "z"])))]
    (test-assembly source-data sink-data assembly)))
