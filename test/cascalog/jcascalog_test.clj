(ns cascalog.jcascalog-test
  (:use clojure.test
        [cascalog api testing])
  (:import [cascalog.test MultiplyAgg RangeOp DoubleOp]
           [jcascalog Api Fields Option Predicate
            PredicateMacro Subquery Api$FirstNArgs]
           [jcascalog.op Avg Count Div Limit Sum Plus Multiply Equals]))

(deftest test-vanilla
  (let [value [["a" 1] ["a" 2] ["b" 10]
               ["c" 3] ["b" 2] ["a" 6]]]
    (test?- [["a" 18] ["b" 24] ["c" 6]]
            (Subquery. (Fields. ["?letter" "?doublesum"])
                       [(Predicate. value (Fields. ["?letter" "?v"]))
                        (Predicate. (Multiply.) (Fields. ["?v" 2]) (Fields. ["?double"]))
                        (Predicate. (Sum.) (Fields. ["?double"]) (Fields. ["?doublesum"])) ]))
    
    (test?- [["a"] ["a"] ["a"]]
            (Subquery. (Fields. ["?letter"])
                       [(Predicate. value (Fields. ["?letter" "_"]))
                        (Predicate. (Equals.) (Fields. ["?letter" "a"]))]))

    (test?- [["a"]]
            (Subquery. (Fields. ["?letter"])
                       [(Predicate. value (Fields. ["?letter" "_"]))
                        (Predicate. #'= (Fields. ["?letter" "a"]))
                        (Option/distinct true)]))

    (test?- [[(* 1 2 3628800 6 2 720) 24]]
            (Subquery. (Fields. ["?result" "?count"])
                       [(Predicate. value (Fields. ["_" "?v"]))
                        (Predicate. (RangeOp.) (Fields. ["?v"]) (Fields. ["?v2"]))
                        (Predicate. (MultiplyAgg.) (Fields. ["?v2"]) (Fields. ["?result"])) 
                        (Predicate. (Count.) (Fields. ["?count"])) ]))))

(def my-avg
  (reify PredicateMacro
    (getPredicates [this [val] [avg]]
      (let [count-var (Api/genNullableVar)
            sum-var (Api/genNullableVar)]
        [(Predicate. (Count.) (Fields. [count-var]))
         (Predicate. (Sum.) (Fields. [val]) (Fields. [sum-var]))
         (Predicate. (Div.) (Fields. [sum-var count-var]) (Fields. [avg]))]))))

(deftest test-java-predicate-macro
  (let [nums [[1] [2] [3] [4] [5]]]
    (test?- [[3]]
            (Subquery. (Fields. ["?avg"])
                       [(Predicate. nums (Fields. ["?v"]))
                        (Predicate. my-avg (Fields. ["?v"]) (Fields. ["?avg"]))]))))

(deftest test-first-n
  (let [data [["a" 1] ["a" 1] ["b" 1] ["c" 1] ["c" 1] ["a" 1]
              ["d" 1]]
        sq (Subquery. (Fields. ["?l" "?count"])
                      [(Predicate. data (Fields. ["?l" "_"]))
                       (Predicate. (Count.) (Fields. ["?count"]))
                       ])
        firstn (Api/firstN sq 2 (-> (Api$FirstNArgs.) (.sort "?count") (.reverse true)))]
    (test?- [["c"]]
            (Subquery. (Fields. ["?l"])
                       [(Predicate. firstn (Fields. ["?l" 2]))]))))

(deftest test-java-each
  (let [data [[1 2 3] [4 5 6]]]
    (test?- [[2 4 6] [8 10 12]]
            (Subquery. (Fields. ["?x" "?y" "?z"])
                       [(Predicate. data (Fields. ["?a" "?b" "?c"]))
                        (Predicate. (Api/each (DoubleOp.)) (Fields. ["?a" "?b" "?c"]) (Fields. ["?x" "?y" "?z"]))]))))
