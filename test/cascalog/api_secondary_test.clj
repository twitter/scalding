(ns cascalog.api-secondary-test
  (:use clojure.test
        cascalog.api
        cascalog.testing)
  (:import [cascading.tuple Fields])
  (:require [cascalog.ops :as c]
            [cascalog.io :as io]))

(deftest test-outfields-query
  (let [age [["nathan" 25]]]
    (is (= ["?age"]
           (get-out-fields
            (<- [?age] (age _ ?age)))))
    (is (= ["!!age2" "!!age"]
           (get-out-fields (<- [!!age2 !!age]
                               (age ?person !!age)
                               (age ?person !!age2)))))
    (is (= ["?person" "!a"]
           (get-out-fields (<- [?person !a]
                               (age ?person !a)))))
    (is (= ["!a" "!count"] (get-out-fields (<- [!a !count]
                                               (age _ !a)
                                               (c/count !count)))))))

(deftest test-outfields-tap
  (is (thrown? AssertionError
               (get-out-fields (memory-source-tap Fields/ALL []))))
  (is (= ["!age"]
         (get-out-fields (memory-source-tap ["!age"] []))))
  (is (= ["?age" "field2"]
         (get-out-fields (memory-source-tap ["?age" "field2"] [])))))

(defbufferop sum+1 [tuples]
  [(inc (reduce + (map first tuples)))])

(defn op-to-pairs [sq op]
  (<- [?c] (sq ?a ?b)
      (op ?a ?b :> ?c)
      (:distinct false)))

(deftest test-use-var
  (let [nums [[1 1] [2 2] [1 3]]]
    (test?<- [[2] [4] [4]]
             [?sum]
             (nums ?a ?b)
             (#'+ ?a ?b :> ?sum)
             (:distinct false))
    (test?- [[0] [0] [-2]] (op-to-pairs nums #'-)
            [[5]]          (op-to-pairs nums #'sum+1)
            [[5]]          (op-to-pairs nums sum+1))))

(deftest test-construct
  (let [age [["alice" 25] ["bob" 30]]
        gender [["alice" "f"]
                ["charlie" "m"]]]
    (test?- [["alice" 26 "f"]
             ["bob" 31 nil]]
            (apply construct
                   ["?p" "?a2" "!!g"]
                   [(-> [[age "?p" "?a"] [#'inc "?a" :> "?a2"]]
                        (conj [gender "?p" "!!g"]))]))))

(deftest test-cascalog-tap-source
  (io/with-log-level :fatal
    (let [num [[1]]
          gen (<- [?n] (num ?raw) (inc ?raw :> ?n) (:distinct false))
          tap1 (cascalog-tap num nil)]
      (test?<- [[1]] [?n] (tap1 ?n) (:distinct false))
      (test?<- [[2]] [?n] ((cascalog-tap gen nil) ?n) (:distinct false))
      (test?<- [[1]] [?n] ((cascalog-tap (cascalog-tap tap1 nil) nil) ?n) (:distinct false)))))

(deftest test-cascalog-tap-sink
  (io/with-log-level :fatal
    (let [num    [[2]]]
      (with-expected-sinks [sink1 [[2]]
                            sink2 [[3]]
                            sink3 [[2]]]
        (?<- (cascalog-tap nil sink1)
             [?n]
             (num ?n)
             (:distinct false))

        (?<- (cascalog-tap nil (fn [sq] [sink2 (<- [?n2]
                                                  (sq ?n)
                                                  (inc ?n :> ?n2)
                                                  (:distinct false))]))
             [?n] (num ?n)
             (:distinct false))

        (?<- (cascalog-tap nil (cascalog-tap nil sink3))
             [?n]
             (num ?n)
             (:distinct false))))))

(deftest test-cascalog-tap-source-and-sink
  (io/with-log-level :fatal
    (with-expected-sinks [sink1 [[4]]]
      (let [tap (cascalog-tap [[3]] sink1)]
        (?<- tap [?n]
             (tap ?raw)
             (inc ?raw :> ?n)
             (:distinct false))))))

(deftest test-symmetric-ops
  (let [nums [[1 2 3] [10 20 30] [100 200 300]]]
    (test?<- [[111 222 333 1 2 3 100 200 300]]
             [?s1 ?s2 ?s3 ?min1 ?min2 ?min3 ?max1 ?max2 ?max3]
             (nums ?a ?b ?c)
             (c/sum ?a ?b ?c :> ?s1 ?s2 ?s3)
             (c/min ?a ?b ?c :> ?min1 ?min2 ?min3)
             (c/max ?a ?b ?c :> ?max1 ?max2 ?max3))))

(deftest test-first-n
  (let [sq (name-vars [[1 1] [1 3] [1 2] [2 1] [3 4]]
                      ["?a" "?b"])]
    (test?- [[1 1] [1 2]]
            (c/first-n sq 2 :sort ["?a" "?b"]))
    (test?- [[3 4] [2 1]]
            (c/first-n sq 2 :sort "?a" :reverse true))
    (is (= 2 (count (first (??- (c/first-n sq 2))))))))

(deftest test-flow-name
  (let [nums [[1] [2]]]
    (with-expected-sinks [sink1 [[1] [2]]
                          sink2 [[2] [3]]]
      (is (= "lalala"
             (.getName (compile-flow "lalala" (stdout) (<- [?n] (nums ?n))))))
      (?<- "flow1" sink1
           [?n]
           (nums ?n)
           (:distinct false))
      (?- "flow2" sink2
          (<- [?n2]
              (nums ?n)
              (inc ?n :> ?n2)
              (:distinct false))))))

(deftest test-data-structure
  (let [src  [[1 5] [5 6] [8 2]]
        nums [[1] [2]]]
    (test?<- [[1 5]]
             [?a ?b]
             (nums ?a)
             (src ?a ?b))))

(deftest test-memory-returns
  (let [nums [[1] [2] [3]]
        people [["alice"] ["bob"]]]
    (is (= (set [[1] [3]])
           (set (??<- [?num]
                      (nums ?num)
                      (odd? ?num)
                      (:distinct false)))))
    (let [res (??- (<- [?val]
                       (nums ?num)
                       (inc ?num :> ?val)
                       (:distinct false))
                   (<- [?res]
                       (people ?person)
                       (str ?person "a" :> ?res)
                       (:distinct false)))]
      (is (= (set [[2] [3] [4]])
             (set (first res))))
      (is (= (set [["alicea"] ["boba"]])
             (set (second res)))))))

(deftest test-negation
  (let [age    [["nathan" 25] ["nathan" 24]
                ["alice" 23] ["george" 31]]
        gender [["nathan" "m"] ["emily" "f"]
                ["george" "m"] ["bob" "m"]]
        follows [["nathan" "bob"] ["nathan" "alice"]
                 ["alice" "nathan"] ["alice" "jim"]
                 ["bob" "nathan"]]]
    (test?<- [["george"]]
             [?p]
             (age ?p _)
             (follows ?p _ :> false)
             (:distinct false))
    (test?<- [["nathan"] ["nathan"]
              ["alice"]]
             [?p]
             (age ?p _)
             (follows ?p _ :> true)
             (:distinct false))
    (test?<- [["alice"]]
             [?p]
             (age ?p _)
             (follows ?p "nathan" :> true)
             (:distinct false))
    (test?<- [["nathan"] ["nathan"]
              ["george"]]
             [?p]
             (age ?p _)
             (follows ?p "nathan" :> false)
             (:distinct false))
    (test?<- [["nathan" true true] ["nathan" true true]
              ["alice" true false] ["george" false true]]
             [?p ?isfollows ?ismale]
             (age ?p _)
             (follows ?p _ :> ?isfollows)
             (gender ?p "m" :> ?ismale)
             (:distinct false))
    (test?<- [["nathan" true true]
              ["nathan" true true]]
             [?p ?isfollows ?ismale]
             (age ?p _)
             (follows ?p _ :> ?isfollows)
             (gender ?p "m" :> ?ismale)
             (= ?ismale ?isfollows)
             (:distinct false))
    (let [old (<- [?p ?a]
                  (age ?p ?a)
                  (> ?a 30)
                  (:distinct false))]
      (test?<- [["nathan"] ["bob"]]
               [?p]
               (gender ?p "m")
               (old ?p _ :> false)
               (:distinct false)))
    (test?<- [[24] [31]]
             [?n]
             (age _ ?n)
             ([[25] [23]] ?n :> false)
             (:distinct false))
    (test?<- [["alice"]]
             [?p]
             (age ?p _)
             ((c/negate gender) ?p _)
             (:distinct false))))

;; TODO: test within massive joins (more than one join field, after other joins complete, etc.)

(deftest test-negation-errors
  (let [nums [[1] [2] [3]]
        pairs [[3 4] [4 5]]]
    (thrown?<- Exception
               [?n]
               (nums ?n)
               (pairs ?n ?n2 :> false)
               (odd? ?n2))))

(defmultibufferop count-sum [seq1 seq2]
  [[(count seq1) (reduce + (map second seq2))]])

(defmultibufferop [count-arg [arg]] [seq1 seq2 seq3]
  [[(count seq1) (count seq2) (count seq3)]
   [arg arg arg]])

(deftest test-multigroup
  (let [val1 [["a" 1] ["b" 2] ["a" 10]]
        val2 [["b" 6] ["b" 18]
              ["c" 3] ["c" 4]]
        gen1 (name-vars val1 ["?key" "?val1"])
        gen2 (name-vars val2 ["?key" "?val2"])
        gen3 (name-vars val1 ["?key" "?val2"])]
    (test?- [["a" 2 0] ["b" 1 24] ["c" 0 7]]
            (multigroup [?key]
                        [?count ?sum]
                        count-sum gen1 gen2))

    (test?- [["a" 2 2 0] ["a" 9 9 9]
             ["b" 1 1 2] ["b" 9 9 9]
             ["c" 0 0 2] ["c" 9 9 9]]
            (multigroup [?key]
                        [?v1 ?v2 ?v3]
                        [count-arg [9]]
                        gen1 gen1 gen2))))
