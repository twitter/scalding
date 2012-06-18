(ns cascalog.pred-macro-test
  (:use clojure.test
        cascalog.testing
        cascalog.api
        [midje sweet cascalog])
  (:import [cascading.tuple Fields])
  (:require [cascalog.ops :as c]
            [cascalog.io :as io]))

(def mac2
  (<- [:< ?a]
    (* ?a ?a :> ?a)))

(deftest test-predicate-macro
  (let [mac1 (<- [?a :> ?b ?c]
                 (+ ?a 1 :> ?t)
                 (* ?t 2 :> ?b)
                 (+ ?a ?t :> ?c))
        mac3 (<- [?a :> ?b]
                 (+ ?a ?a :> ?b))
        num1 [[0] [1] [2] [3]]]
    (test?<- [[-1 1] [0 3] [1 5] [2 7]]
             [?t ?o]
             (num1 ?n)
             (mac1 ?n :> _ ?o)
             (dec ?n :> ?t))

    (test?<- [[0] [1]]
             [?n]
             (num1 ?n)
             (#'mac2 ?n))

    ;; test that it allows same var used as input and output
    (test?<- [[0]]
             [?n]
             (num1 ?n)
             (mac3 ?n :> ?n))))

(defn splitter [^String str]
  (seq (.split str " ")))

(deftest test-destructuring
  (let [triplets [[1 2 3] [2 3 4] [3 3 5]]
        pairs    [["a b"] ["c d"]]
        singles  [["a"] ["b"]]
        str-mac (<- [:<< !all :> !val]
                    (str :<< !all :> !i)
                    (str !i "a" :> !val))
        complex-mac (<- [:<< [!a !b & !rest :as !all] :>> [!v1 !v2 !v3]]
                        (str :<< !all :> !v1)
                        (str !a !b :> !v2)
                        (str :<< !rest :> !v3))
        var-out (<- [!a :>> !allout]
                    (str !a "a" :> !b)
                    (splitter !b :>> !allout))]

    (test?<- [["123a"] ["234a"] ["335a"]]
             [!out]
             (triplets !a !b !c)
             (str-mac !a !b !c :> !out))

    (test?<- [["32a"] ["43a"] ["53a"]]
             [!out]
             (triplets _ !b !c)
             (str-mac !c !b :> !out))

    (test?<- [["123" "12" "3"] ["234" "23" "4"] ["335" "33" "5"]]
             [!o1 !o2 !o3]
             (triplets !a !b !c)
             (complex-mac !a !b !c :> !o1 !o2 !o3))


    (future-fact?<-
     "this test doesn't work because cascading doesn't allow
      functions with no input"
     [["12" ""] ["23" ""] ["33" ""]]
            [!o2 !o3]
            (triplets !a !b _) (complex-mac !a !b :> !o1 !o2 !o3))

    (test?<- [["12" "12"] ["23" "23"] ["33" "33"]]
             [!o1 !o2]

             (triplets !a !b _)
             (complex-mac !a !b :> !o1 !o2 !o3))

    (test?<- [["a" "ba"] ["c" "da"]]
             [!a !b]
             (pairs !i)
             (var-out !i :> !a !b))

    (test?<- [["aa"] ["ba"]]
             [!a]
             (singles !i)
             (var-out !i :> !a))))

(future-fact " TODO: test construct with destructuring, esp. string &")

(defn odd-sum? [& params]
  (odd? (reduce + params)))

(defn mult-3-sum? [& params]
  (let [s (reduce + params)]
    (zero? (mod s 3))))

(defn large-total? [& params]
  (-> (reduce + params)
      (> 10)))

(defn double-num [val]
  (* 2 val))

(deftest test-composites
  (let [nums [[1 2] [3 3] [4 6]]]
    (test?<- [[1 2] [3 3]] [!a !b]
             (nums !a !b)
             ((c/any #'odd-sum? #'mult-3-sum? #'large-total?) !a !b))

    (test?<- [[3 3] [4 6]] [!a !b]
             (nums !a !b)
             ((c/any #'odd-sum? #'large-total?) !a !b !a))

    (test?<- [[1 2]] [!a !b]
             (nums !a !b)
             ((c/any #'odd-sum? #'large-total?) !a !b !a :> false))
    
    (test?<- [[1 2]] [!a !b]
             (nums !a !b)
             ((c/all #'odd-sum? #'large-total? #'mult-3-sum?) !a !b !b !b !b !b !b !b))

    (test?<- [[3 3]] [!a !b]
             (nums !a !b)
             ((c/all #'odd-sum? #'mult-3-sum?) !a))
    
    (test?<- [[2] [6]]
             [!a]
             (nums _ !a)
             ((c/negate #'odd?) !a))

    (test?<- [[3]]
             [!a]
             (nums !a !b)
             ((c/negate #'<) !a !b))
    
    (test?<- [[true] [false] [false]]
             [!c]
             (nums !a !b)
             ((c/comp #'odd? #'+) !a !b :> !c)
             (:distinct false))

    (test?<- [[true] [true] [false]]
             [!c]
             (nums !a !b)
             ((c/comp #'not #'odd? #'+) !a !b :> !c)
             (:distinct false))

    (test?<- [[5] [9] [11]] [!c]
             (nums !a _)
             ((c/comp #'inc #'double-num #'inc) !a :> !c)
             (:distinct false))

    (test?<- [[2] [4] [5]]
             [!c]
             (nums !a _)
             ((c/comp #'inc) !a :> !c)
             (:distinct false))

    (test?<- [[2 2] [4 6] [5 8]]
             [!v1 !v2]
             (nums !a !b)
             ((c/juxt #'inc #'double-num) !a :> !v1 !v2))

    (test?<- [[3 -1 true] [6 0 false] [10 -2 true]]
             [!v1 !v2 !v3]
             (nums !a !b)
             ((c/juxt #'+ #'- #'<) !a !b :> !v1 !v2 !v3))))

(defn append-! [v]
  (str v "!"))

(deffilterop small-op? [v]
  (< v 4))

(deftest test-each
  (let [triples [[1 2 3] [3 4 1]]]
    (test?<-[["1!" "2!"] ["3!" "4!"]]
            [!v1 !v2]
            (triples !a !b !c)
            ((c/each #'append-!) !a !b :> !v1 !v2)
            (:distinct false))

    (test?<-[["2!"] ["4!"]]
            [!v]
            (triples !a !b !c)
            ((c/each #'append-!) !b :> !v)
            (:distinct false))

    (test?<-[[3 1 2]]
            [!c !a !b]
            (triples !a !b !c)
            ((c/each small-op?) !a !b !c)
            (:distinct false))))

(deftest test-nested-predmacro
  (let [integers [[1] [4]]
        pm1 (predmacro [invars outvars]
                       (map (fn [i v] [#'append-! i :> v]) invars outvars))
        pm2 (predmacro [invars outvars]
                       [[pm1 :<< invars :>> outvars]
                        [small-op? (first invars)]])]
    (test?<- [["1!"]]
             [?v]
             (integers ?i)
             (pm2 ?i :> ?v))))

(defn small? [num]
  (< num 10))

(deftest test-composite-composites
  (let [nums [[1 2] [3 3] [4 6] [6 8] [-2 -1]]]
    (test?<- [[4] [-2]]
             [!a]
             (nums !a _)
             ((c/negate (c/any #'odd? #'mult-3-sum?)) !a))

    (test?<- [[3] [4] [6]]
             [!a]
             (nums !a _)
             ((c/any
               (c/all #'odd? #'mult-3-sum?)
               (c/all #'even? #'pos? #'small?)) !a))
    (future-fact "TODO: not all any")))

(deftest test-errors
  (thrown?<- RuntimeException
             [?a ?b ?c :> ?d ?a]
             (+ ?a ?b ?c :> ?d)))
