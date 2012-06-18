(ns cascalog.api-test
  (:use clojure.test
        [midje sweet cascalog]
        [cascalog testing api])
  (:require [cascalog.ops :as c]
            [cascalog.util :as u])
  (:import [cascading.tuple Fields]
           [cascalog.test KeepEven OneBuffer CountAgg SumAgg]
           [cascalog.ops IdentityBuffer]))

(defmapop mk-one
  "Returns 1 for any input."
  [& tuple] 1)

(deftest test-no-input
  (let [nums [[1] [2] [3]]]
    (test?<- [[1 1] [2 1] [3 1]]
             [?n ?n2]
             (nums ?n)
             (mk-one ?n2))
    (test?<- [[1 1] [1 2] [1 3]
              [2 1] [2 2] [2 3]
              [3 1] [3 2] [3 3]]
             [?n ?n3]
             (nums ?n)
             (mk-one ?n2)
             (nums ?n3))))

(deftest test-simple-query
  (let [age [["n" 24] ["n" 23] ["i" 31] ["c" 30] ["j" 21] ["q" nil]]]
    (test?<- [["j"] ["n"]]
             [?p]
             (age ?p ?a)
             (< ?a 25)
             (:distinct true))
    (test?<- [["j"] ["n"] ["n"]]
             [?p]
             (age ?p ?a)
             (< ?a 25))))

(deftest test-larger-tuples
  (let [stats [["n" 6 190 nil] ["n" 6 195 nil]
               ["i" 5 180 31] ["g" 5 150 60]]
        friends [["n" "i" 6] ["n" "g" 20]
                 ["g" "i" nil]]]
    (test?<- [["g" 60]]
             [?p ?a]
             (stats ?p _ _ ?a)
             (friends ?p _ _))
    (test?<- []
             [?p ?a]
             (stats ?p 1000 _ ?a))
    (test?<- [["n"] ["n"]]
             [?p]
             (stats ?p _ _ nil)
             (:distinct false))))

(deftest test-multi-join
  (let [age [["n" 24] ["a" 15] ["j" 24]
             ["d" 24] ["b" 15]
             ["z" 62] ["q" 24]]
        friends [["n" "a" 16] ["n" "j" 12]
                 ["j" "n" 10] ["j" "d" nil]
                 ["d" "q" nil] ["b" "a" nil]
                 ["j" "a" 1] ["a" "z" 1]]]
    (test?<- [["n" "j"] ["j" "n"]
              ["j" "d"] ["d" "q"] ["b" "a"]]
             [?p ?p2]
             (age ?p ?a)
             (age ?p2 ?a)
             (friends ?p ?p2 _))))

(deftest test-many-joins
  (let [age-prizes [[10 "toy"] [20 "animal"]
                    [30 "car"] [40 "house"]]
        friends    [["n" "j"] ["n" "m"]
                    ["n" "a"] ["j" "a"]
                    ["a" "z"]]
        age         [["z" 20] ["a" 10]
                     ["n" 15]]]
    (test?<- [["n" "animal-!!"] ["n" "house-!!"]
              ["j" "house-!!"]]
             [?p ?prize]
             (friends ?p ?p2)
             (friends ?p2 ?p3)
             (age ?p3 ?a)
             (* 2 ?a :> ?a2)
             (age-prizes ?a2 ?prize2)
             (str ?prize2 "-!!" :> ?prize))))

(deftest test-bloated-join
  (let [gender     [["n" "male"] ["j" "male"]
                    ["a" nil] ["z" "female"]]
        interest   [["n" "bball"] ["n" "dl"] ["j" "tennis"]
                    ["z" "stuff"] ["a" "shoes"]]
        friends    [["n" "j"] ["n" "m"] ["n" "a"]
                    ["j" "a"] ["a" "z"] ["z" "a"]]
        age        [["z" 20] ["a" 10] ["n" 15]]]
    (test?<- [["n" "bball" 15 "male"] ["n" "dl" 15 "male"]
              ["a" "shoes" 10 nil] ["z" "stuff" 20 "female"]]
             [!p !interest !age !gender]
             (friends !p _)
             (age !p !age)
             (interest !p !interest)
             (gender !p !gender)
             (:distinct true))))

(defmapcatop split [^String words]
  (seq (.split words "\\s+")))

(deftest test-countall
  (let [sentence [["hello this is a"]
                  ["say hello hello to the man"]
                  ["this is the cool beans man"]]]
    (test?<- [["hello" 3] ["this" 2] ["is" 2]
              ["a" 1] ["say" 1] ["to" 1] ["the" 2]
              ["man" 2] ["cool" 1] ["beans" 1]]
             [?w ?c]
             (sentence ?s)
             (split ?s :> ?w)
             (c/count ?c))))

(deftest test-multi-sink)

(deftest test-multi-agg
  (let [value [["a" 1] ["a" 2] ["b" 10]
               ["c" 3] ["b" 2] ["a" 6]] ]
    (test?<- [["a" 12] ["b" 14] ["c" 4]]
             [?v ?a]
             (value ?v ?n)
             (c/count ?c)
             (c/sum ?n :> ?s)
             (+ ?s ?c :> ?a))))

(deftest test-joins-aggs
  (let [friend [["n" "a"] ["n" "j"] ["n" "q"]
                ["j" "n"] ["j" "a"]
                ["j" "z"] ["z" "t"]]
        age    [["n" 25] ["z" 26] ["j" 20]] ]
    (test?<- [["n"] ["j"]]
             [?p]
             (age ?p _)
             (friend ?p _)
             (c/count ?c)
             (> ?c 2))))

(deftest test-global-agg
  (let [num [[1] [2] [5] [6] [10] [12]]]
    (test?<- [[6]]
             [?c]
             (num _)
             (c/count ?c))
    (test?<- [[6 72]]
             [?c ?s2]
             (num ?n)
             (c/count ?c)
             (c/sum ?n :> ?s)
             (* 2 ?s :> ?s2))))

(defaggregateop evens-vs-odds
  "Decrements state for odd inputs, increases for even. Returns final
   state as a 1-tuple."
  ([] 0)
  ([context val] (if (odd? val)
                   (dec context)
                   (inc context)))
  ([context] [context]))

(deftest test-complex-noncomplex-agg-mix
  (let [num [["a" 1] ["a" 2] ["a" 5]
             ["c" 6] ["d" 9] ["a" 12]
             ["c" 16] ["e" 16]] ]
    (test?<- [["a" 4 0 20] ["c" 2 2 22]
              ["d" 1 -1 9] ["e" 1 1 16]]
             [?a ?c ?e ?s]
             (num ?a ?n)
             (c/count ?c)
             (c/sum ?n :> ?s)
             (evens-vs-odds ?n :> ?e))))

(defn mk-agg-test-tuples []
  (conj (vec (take 12000 (iterate (fn [[a b]] [(inc a) b]) [0 1]))) [0 10]))

(defn mk-agg-test-results []
  (conj (vec (take 11999 (iterate (fn [[a b c]]
                                    [(inc a) b c])
                                  [1 1 1])))
        [0 11 2]))

(deftest test-complex-agg-more-than-10000
  (let [num (mk-agg-test-tuples)]
    (test?<- (mk-agg-test-results)
             [?n ?s ?c]
             (num ?n ?v)
             (c/sum ?v :> ?s)
             (c/count ?c))))

(deftest test-multi-rule
  (let [age [["n" 24] ["c" 40] ["j" 23] ["g" 50]]
        interest [["n" "bb" nil] ["n" "fb" 20]
                  ["g" "ck" 30] ["j" "nz" 10]
                  ["j" "hk" 1] ["jj" "ee" nil]]
        follows [["n" "j"] ["j" "n"] ["j" "a"] ["n" "a"] ["g" "q"]]
        many-follow      (<- [?p]
                             (follows ?p _)
                             (c/count ?c)
                             (> ?c 1))
        active-follows   (<- [?p ?p2]
                             (many-follow ?p)
                             (many-follow ?p2)
                             (follows ?p ?p2))
        unknown-interest (<- [?p]
                             (age ?p ?a)
                             (interest ?p _ !i)
                             (nil? !i))
        weird-follows    (<- [?p ?p2]
                             (active-follows ?p ?p2)
                             (unknown-interest ?p2))]
    (test?- [["n" "j"] ["j" "n"]] active-follows
            [["j" "n"]]           weird-follows
            [["n"]]               unknown-interest)))

(deftest test-filter-same-field
  (let [nums [[1 1] [0 0] [1 2] [3 7] [8 64] [7 1] [2 4] [6 6]]]
    (test?<- [[1] [0] [6]]
             [?n]
             (nums ?n ?n))
    (test?<- [[1 1] [0 0] [8 64] [2 4]]
             [?n ?n2]
             (nums ?n ?n2)
             (* ?n ?n :> ?n2))
    (test?<- [[0]]
             [?n]
             (nums ?n ?n)
             (* ?n ?n :> ?n)
             (+ ?n ?n :> ?n))
    (test?<- [[1 1] [1 2] [0 0] [6 6]]
             [?n ?n2]
             (nums ?n ?n)
             (nums ?n ?n2))
    (test?<- [[14]]
             [?s]
             (nums ?n ?n)
             (* 2 ?n :> ?n2)
             (c/sum ?n2 :> ?s))
    (test?<- [[6] [0]]
             [?n2]
             (nums ?n ?n)
             (nums ?n2 ?n2)
             (* 6 ?n :> ?n2))))

(defbufferop select-first [tuples]
  [(first tuples)])

(deftest test-sort
  (let [pairs [["a" 1] ["a" 2] ["a" 3] ["b" 10] ["b" 20]]]
    (test?<- [["a" 1] ["b" 10]]
             [?f1 ?f2]
             (pairs ?f1 ?v)
             (:sort ?v)
             (select-first ?v :> ?f2))
    
    (test?<- [["a" 3] ["b" 20]]
             [?f1 ?f2]
             (pairs ?f1 ?v)
             (:sort ?v)
             (:reverse true)
             (select-first ?v :> ?f2))))

(defn existence2str [obj]
  (if obj "some" "none"))

(defmapop outer-join-tester [obj]
  (if obj "o" "n"))

(defmapop outer-join-tester2 [obj]
  (if obj "o2" "n2"))

(defmapcatop outer-join-tester3 [obj]
  (if obj [1] [1 1]))

(deftest test-outer-join-basic
  (let [person [["a"] ["b"] ["c"] ["d"]]
        follows [["a" "b"] ["c" "e"] ["c" "d"]]]
    (test?<- [["a" "b"] ["c" "e"] ["c" "d"]
              ["b" nil] ["d" nil]]
             [?p !!p2]
             (person ?p)
             (follows ?p !!p2))

    (test?<- [["a" "b" "b"] ["c" "e" "d"] ["c" "e" "e"]
              ["c" "d" "d"] ["c" "d" "e"] ["b" nil nil]
              ["d" nil nil]]
             [?p !!p2 !!p3]
             (person ?p)
             (follows ?p !!p2)
             (follows ?p !!p3))

    (test?<- [["a" 1 1] ["c" 2 2]
              ["b" 0 1] ["d" 0 1]]
             [?p ?c ?t]
             (person ?p)
             (follows ?p !!p2)
             (c/!count !!p2 :> ?c)
             (c/count ?t))

    (test?<- [["a" "some"] ["b" "none"]
              ["c" "some"] ["d" "none"]]
             [?p ?s]
             (person ?p)
             (follows ?p !!p2)
             (existence2str !!p2 :> ?s)
             (:distinct true))))

(deftest test-outer-join-complex
  (let [age [["a" 20] ["b" 30]
             ["c" 27] ["d" 40]]
        rec1 [["a" 1 2] ["b" 30 16] ["e" 3 4]]
        rec2 [["a" 20 6] ["c" 27 25]
              ["c" 1 11] ["f" 30 1]
              ["b" 100 16]]]
    (test?<- [["a" 20 1 2 6] ["c" 27 nil nil 25]
              ["d" 40 nil nil nil] ["b" 30 30 16 nil]]
             [?p ?a !!f1 !!f2 !!f3]
             (age ?p ?a)
             (rec1 ?p !!f1 !!f2)
             (rec2 ?p ?a !!f3))))

(deftest test-outer-join-assertions
  (let [age [["a" 20] ["b" 30] ["c" 27] ["d" 40]]
        rec1 [["a" 1 2] ["b" 30 16] ["e" 3 4]]]

    "Each unground var can only appear in one generator."
    (thrown?<- IllegalArgumentException
               [!!a ?c]
               (age !!a ?b)
               (rec1 !!a ?f1 ?f2)
               (- ?b 2 :> ?c))

    "Ungrounding vars have to spring from a generator."
    (thrown?<- IllegalArgumentException
               [!!a !!c]
               (age !!a ?b)
               (- ?b 2 :> !!c))

    "No ungrounding vars allowed in generators-as-sets."
    (thrown?<- IllegalArgumentException
               [!!a]
               (age !!a ?b)
               (rec1 !!a _ _ :> true))
    
    (thrown?<- IllegalArgumentException
               [?a !!c]
               (age ?a ?b)
               (rec1 ?a _ _ :> !!c))))

(deftest test-full-outer-join
  (let [age        [["A" 20] ["B" 30] ["C" 27] ["D" 40]]
        gender     [["A" "m"] ["B" "f"] ["E" "m"] ["F" "f"]]
        follows    [["A" "B"] ["B" "E"] ["B" "G"] ["E" "D"]]
        age-tokens [["B" 20 "d"] ["A" 30 "a"]]]
    (test?<- [["A" 20 "m"] ["B" 30 "f"]
              ["C" 27 nil] ["D" 40 nil]
              ["E" nil "m"] ["F" nil "f"]]
             [?p !!a !!g]
             (age ?p !!a)
             (gender ?p !!g))

    (test?<- [["A" "o" "o2"] ["B" "o" "o2"]
              ["C" "o" "n2"] ["D" "o" "n2"]
              ["E" "n" "o2"] ["F" "n" "o2"]]
             [?p ?s ?s2]
             (age ?p !!a)
             (gender ?p !!g)
             (outer-join-tester !!a :> ?s)
             (outer-join-tester2 !!g :> ?s2))

    (test?<- [["A" 1] ["B" 1] ["C" 1]
              ["D" 1] ["E" 2] ["F" 2]]
             [?p ?c]
             (age ?p !!a)
             (gender ?p !!g)
             (outer-join-tester3 !!a :> ?t)
             (c/count ?c))

    (test?<- [["A" "a"] ["E" nil]]
             [?p !!t]
             (follows ?p ?p2)
             (age ?p2 ?a2)
             (age-tokens ?p ?a2 !!t))

    (test?<- [["E"]]
             [?p]
             (follows ?p ?p2)
             (age ?p2 ?a2)
             (age-tokens ?p ?a2 !!t)
             (nil? !!t))))

(defmapop [hof-add [a]]
  "Adds the static variable `a` to dynamic input `n`."
  [n]
  (+ a n))

(defmapop [hof-arithmetic [a b]] [n]
  (+ b (* a n)))

;; TODO: stateful operations should return a map containing :init,
;; :op, :finish
(defbufferop [sum-plus [a]]
  {:stateful true}
  ([] (* 3 a))
  ([state tuples] [(apply + state (map first tuples))])
  ([state] nil))

(deftest test-hof-ops
  (let [integer [[1] [2] [6]]]
    (test?<- [[4] [5] [9]]
             [?n]
             (integer ?v)
             (hof-add 3 ?v :> ?n))

    (test?<- [[-5] [-4] [0]]
             [?n]
             (integer ?v)
             (hof-add [-6] ?v :> ?n))

    (test?<- [[3] [5] [13]]
             [?n]
             (integer ?v)
             (hof-arithmetic [2 1] ?v :> ?n))

    (test?<- [[72]]
             [?n]
             (integer ?v)
             (sum-plus [21] ?v :> ?n))))

(defn lala-appended [source]
  (let [outvars ["?a"]]
    (<- outvars
        (source ?line)
        (str ?line "lalala" :>> outvars)
        (:distinct false))))

(deftest test-dynamic-vars
  (let [sentence [["nathan david"] ["chicken"]]]
    (test?<- [["nathan davidlalala"] ["chickenlalala"]]
             [?out]
             ((lala-appended sentence) ?out))
    
    (test?<- [["nathan davida"] ["chickena"]]
             [?out]
             (sentence :>> [?line])
             (str :<< ["?line" "a"] :>> ["?out"]))))

(defbufferop nothing-buf [tuples] tuples)

(deftest test-limit
  (let [pair [["a" 1] ["a" 3] ["a" 2]
              ["a" 4] ["b" 1] ["b" 6]
              ["b" 7] ["c" 0]]]
    (test?<- [[0] [1] [1] [2]
              [3] [4] [6] [7]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             (nothing-buf ?n :> ?n2))

    (test?<- [[0] [1]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             (c/limit [2] ?n :> ?n2))

    (test?<- [[1 1] [2 2] [3 3]
              [4 4] [1 5]]
             [?n2 ?r]
             (pair ?l ?n)
             (:sort ?l ?n)
             (c/limit-rank [5] ?n :> ?n2 ?r))

    (test?<- [["c" 0] ["b" 7]]
             [?l2 ?n2]
             (pair ?l ?n)
             (:sort ?l ?n)
             (:reverse true)
             (c/limit [2] ?l ?n :> ?l2 ?n2))

    (test?<- [[0] [1] [1]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             (c/limit [3] ?n :> ?n2))

    (test?<- [[0 1] [1 2] [1 3]]
             [?n2 ?r]
             (pair _ ?n)
             (:sort ?n)
             (c/limit-rank [3] ?n :> ?n2 ?r))

    (test?<- [[6] [7]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             (:reverse true)
             (c/limit [2] ?n :> ?n2))

    (test?<- [[6 2] [7 1]]
             [?n2 ?r]
             (pair _ ?n)
             (:sort ?n)
             (:reverse true)
             (c/limit-rank [2] ?n :> ?n2 ?r))

    (test?<- [["a" 1] ["a" 2] ["b" 1]
              ["b" 6] ["c" 0]]
             [?l ?n2]
             (pair ?l ?n)
             (:sort ?n)
             (c/limit [2] ?n :> ?n2))))

(deftest test-outer-join-anon
  (let [person  [["a"] ["b"] ["c"]]
        follows [["a" "b" 1] ["c" "e" 2] ["c" "d" 3]]]
    (test?<- [["a" "b"] ["c" "e"]
              ["c" "d"] ["b" nil]]
             [?p !!p2]
             (person ?p)
             (follows ?p !!p2 _))))

(defbufferiterop itersum [tuples-iter]
  [(->> (iterator-seq tuples-iter)
        (map first)
        (reduce +))])

(deftest test-bufferiter
  (let [nums [[1] [2] [4]]]
    (test?<- [[7]]
             [?s]
             (nums ?n)
             (itersum ?n :> ?s))))

(defn inc-tuple [& tuple]
  (map inc tuple))

(deftest test-pos-out-selectors
  (let [wide [["a" 1 nil 2 3] ["b" 1 "c" 5 1] ["a" 5 "q" 3 2]]]
    (test?<- [[3 nil] [1 "c"] [2 "q"]]
             [?l !m]
             (wide :#> 5 {4 ?l 2 !m})
             (:distinct false))
    
    (test?<- [[4] [2] [3]]
             [?n3]
             (wide _ ?n1 _ _ ?n2)
             (inc-tuple ?n1 ?n2 :#> 2 {1 ?n3}))

    (test?<- [["b"]]
             [?a]
             (wide :#> 5 {0 ?a 1 ?b 4 ?b}))))

(deftest test-avg
  (let [num1 [[1] [2] [3] [4] [10]]
        pair [["a" 1] ["a" 2] ["a" 3] ["b" 3]]]
    (test?<- [[4]]
             [?avg]
             (num1 ?n)
             (c/avg ?n :> ?avg))

    (test?<- [["a" 2] ["b" 3]]
             [?l ?avg]
             (pair ?l ?n)
             (c/avg ?n :> ?avg))))

(deftest test-distinct-count
  (let [num1 [[1] [2] [2] [4] [1] [6] [19] [1]]
        pair [["a" 1] ["a" 2] ["b" 3] ["a" 1]]]
    (test?<- [[5]]
             [?c]
             (num1 ?n)
             (c/distinct-count ?n :> ?c))

    (test?<- [["a" 2] ["b" 1]]
             [?l ?c]
             (pair ?l ?n)
             (c/distinct-count ?n :> ?c))))

(deftest test-nullable-agg
  (let [follows [["a" "b"] ["b" "c"] ["a" "c"]]]
    (test?<- [["a" 2] ["b" 1]]
             [?p !c]
             (follows ?p _)
             (c/count !c))))

(deffilterop odd-fail [n & all]
  (or (even? n)
      (throw (RuntimeException.))))

(deffilterop a-fail [n & all]
  (if (= "a" n)
    (throw (RuntimeException.)) true))

(deftest memory-self-join-test
  (let [src [["a"]]
        src2 (memory-source-tap [["a"]])]
    (with-expected-sink-sets [empty1 [], empty2 []]
      (test?<- src
               [!a]
               (src !a)
               (src !a)
               (:distinct false)
               (:trap empty1))

      (test?<- src
               [!a]
               (src2 !a)
               (src2 !a)
               (:distinct false)
               (:trap empty2)))))

(deftest test-trap
  (let [num [[1] [2]]]
    (with-expected-sink-sets [trap1 [[1]] ]
      (test?<- [[2]]
               [?n]
               (num ?n)
               (odd-fail ?n)
               (:trap trap1)))
    
    (is (thrown? Exception (test?<- [[2]]
                                    [?n]
                                    (num ?n)
                                    (odd-fail ?n))))))

(deftest test-trap-joins
  (let [age    [["A" 20] ["B" 21]]
        gender [["A" "m"] ["B" "f"]]]
    (with-expected-sink-sets [trap1 [[21]]
                              trap2 [[21 "f"]]]
      (test?<- [["A" 20 "m"]]
               [?p ?a ?g]
               (age ?p ?a)
               (gender ?p ?g)
               (odd-fail ?a)
               (:trap trap1))

      (test?<- [["A" 20 "m"]]
               [?p ?a ?g]
               (age ?p ?a)
               (gender ?p ?g)
               (odd-fail ?a ?g)
               (:trap trap2)))))

(deftest test-multi-trap
  (let [age [["A" 20] ["B" 21]]
        weight [["A" 191] ["B" 192]]]
    (with-expected-sink-sets [trap1 [[21]]
                              trap2 [["A" 20 191]] ]
      (let [sq (<- [?p ?a]
                   (age ?p ?a)
                   (odd-fail ?a)
                   (:trap trap1)
                   (:distinct false))]
        (test?<- []
                 [?p ?a ?w]
                 (sq ?p ?a)
                 (weight ?p ?w)
                 (odd-fail ?w ?p ?a)
                 (:trap trap2))))))

(deftest test-trap-isolation
  (let [num [[1] [2]]]
    (is (thrown? Exception
                 (with-expected-sink-sets [trap1 [[]] ]
                   (let [sq (<- [?n] (num ?n) (odd-fail ?n))]
                     (test?<- [[2]]
                              [?n]
                              (sq ?n)
                              (:trap trap1))))))
    (with-expected-sink-sets [trap1 [[1]]]
      (let [sq (<- [?n]
                   (num ?n)
                   (odd-fail ?n)
                   (:trap trap1))]
        (test?<- [[2]]
                 [?n]
                 (sq ?n))))))

(defn multipagg-init [v1 v2 v3]
  [v1 (+ v2 v3)])

(defn multipagg-combiner [v1 v2 v3 v4]
  [(+ v1 v3)
   (* v2 v4)])

(defparallelagg multipagg
  :init-var #'multipagg-init
  :combine-var #'multipagg-combiner)

(defaggregateop slow-count
  ([] 0)
  ([context val] (inc context))
  ([context] [context]))

(deftest test-multi-parallel-agg
  (let [vals [[1 2 3] [4 5 6] [7 8 9]]]
    (test?<- [[12 935 3]]
             [?d ?e ?count]
             (vals ?a ?b ?c)
             (multipagg ?a ?b ?c :> ?d ?e)
             (c/count ?count))
    
    (test?<- [[12 935 3]]
             [?d ?e ?count]
             (vals ?a ?b ?c)
             (multipagg ?a ?b ?c :> ?d ?e)
             (slow-count ?c :> ?count))))

(deftest test-cascading-filter
  (let [vals [[0] [1] [2] [3]]]
    (test?<- [[0] [2]]
             [?v]
             (vals ?v)
             ((KeepEven.) ?v)
             (:distinct false))
    
    (test?<- [[0 true] [1 false]
              [2 true] [3 false]]
             [?v ?b]
             (vals ?v)
             ((KeepEven.) ?v :> ?b)
             (:distinct false))))

(deftest test-java-buffer
  (let [vals [["a" 1 10] ["a" 2 20] ["b" 3 31]]]
    (test?<- [["a" 1] ["b" 1]]
             [?f1 ?o]
             (vals ?f1 _ _)
             ((OneBuffer.) :> ?o))

    (test?<- [["a" 1 10] ["a" 2 20] ["b" 3 31]]
             [?f1 ?f2out ?f3out]
             (vals ?f1 ?f2 ?f3)
             ((IdentityBuffer.) ?f2 ?f3 :> ?f2out ?f3out))))

(deftest test-java-aggregator
  (let [vals [["a" 1] ["a" 2] ["b" 3] ["c" 8] ["c" 13] ["b" 1] ["d" 5] ["c" 8]]]
    (test?<- [["a" 2] ["b" 2] ["c" 3] ["d" 1]]
             [?f1 ?o]
             (vals ?f1 _)
             ((CountAgg.) :> ?o))

    (test?<- [["a" 3 2] ["b" 4 2] ["c" 29 3] ["d" 5 1]]
             [?key ?sum ?count]
             (vals ?key ?val)
             ((CountAgg.) ?count)
             ((SumAgg.) ?val :> ?sum))))

(defn run-union-combine-tests
  "Runs a series of tests on the union and combine operations. v1,
  v2 and v3 must produce

    [[1] [2] [3]]
    [[3] [4] [5]]
    [[2] [4] [6]]"
  [v1 v2 v3]
  (test?- [[1] [2] [3] [4] [5]]                 (union v1 v2)
          [[1] [2] [3] [4] [5] [6]]             (union v1 v2 v3)
          [[3] [4] [5]]                         (union v2)
          [[1] [2] [3] [2] [4] [6]]             (combine v1 v3)
          [[1] [2] [3] [3] [4] [5] [2] [4] [6]] (combine v1 v2 v3)))

(deftest test-vector-union-combine
  (run-union-combine-tests [[1] [2] [3]]
                           [[3] [4] [5]]
                           [[2] [4] [6]]))

(deftest test-query-union-combine
  (run-union-combine-tests (<- [?v] ([[1] [2] [3]] ?v) (:distinct false))
                           (<- [?v] ([[3] [4] [5]] ?v) (:distinct false))
                           (<- [?v] ([[2] [4] [6]] ?v) (:distinct false))))

(deftest test-cascading-union-combine
  (let [v1 [[1] [2] [3]]
        v2 [[3] [4] [5]]
        v3 [[2] [4] [6]]
        e1 []]
    (run-union-combine-tests v1 v2 v3)

    "Can't use empty taps inside of a union or combine."
    (is (thrown? IllegalArgumentException (union e1)))
    (is (thrown? IllegalArgumentException (combine e1)))))

(deftest test-select-fields-tap
  (let [data (memory-source-tap ["f1" "f2" "f3" "f4"]
                                [[1 2 3 4] [11 12 13 14] [21 22 23 24]])]
    (test?<- [[4 2] [14 12] [24 22]]
             [?a ?b]
             ((select-fields data ["f4" "f2"]) ?a ?b))

    (test?<- [[1 3 4] [11 13 14] [21 23 24]]
             [?f1 ?f2 ?f3]
             ((select-fields data ["f1" "f3" "f4"]) ?f1 ?f2 ?f3))))

(deftest test-keyword-args
  (test?<- [[":onetwo"]]
           [?b]
           ([["two"]] ?a)
           (str :one ?a :> ?b))

  (test?<- [["face"]]
           [?a]
           ([["face" :cake]] ?a :cake)))

(deftest test-select-fields-query
  (let [wide [[1 2 3 4 5 6]]
        sq (<- [!f1 !f4 !f5 ?f6]
               (wide !f1 !f2 !f3 !f4 !f5 ?f6)
               (:distinct false))]
    (test?- [[1]]     (select-fields sq "!f1"))
    (test?- [[1 6]]   (select-fields sq ["!f1" "?f6"]))
    (test?- [[1 6]]   (select-fields sq ["!f1" "?f6"]))
    (test?- [[5 4 6]] (select-fields sq ["!f5" "!f4" "?f6"]))))

(deftest test-function-sink
  (let [pairs [[1 2] [2 10]]
        double-second-sink (fn [sq]
                             [[[1 2 4] [2 10 20]]
                              (<- [?a ?b ?c]
                                  (sq ?a ?b)
                                  (* 2 ?b :> ?c)
                                  (:distinct false)) ])]
    (test?- double-second-sink pairs)))

(deftest test-constant-substitution
  (let [pairs [[1 2] [1 3] [2 5]]]
    (test?<- [[1 2]]
             [?a ?b]
             (pairs ?a ?b)
             (* 2 ?b :> 4)
             (:distinct false))

    (test?<- [[1]]
             [?a]
             (pairs ?a ?b)
             (c/count 2))

    (test?<- [[2]] [?a]
             (pairs ?a _)
             (odd? ?a :> false))

    (future-fact
     "Constants should work in aggregator predicates. See
      https://github.com/nathanmarz/cascalog/issues/26"
     (<- [?a ?count]
         (pairs ?a _)
         (c/sum 2 :> ?count)) => (produces [[1 4] [2 2]]))))

(defmulti multi-test class)
(defmethod multi-test String [x] "string!")
(defmethod multi-test Long [x] "number!")
(defmethod multi-test Integer [x] "number!")
(defmethod multi-test Double [x] "double!")

(deftest test-multimethod-support
  (let [src [["word."] [1] [1.0]]]
    (test?<- [["string!"] ["number!"] ["double!"]]
             [?result]
             (src ?thing)
             (multi-test ?thing :> ?result))))

(defmapop [var-apply [v]]
  "Applies the supplied var v to the supplied `xs`."
  [& xs]
  (apply v xs))

(deftest test-var-constants
  (let [coll-src [[[3 2 4 1]]
                  [[1 2 3 4 5]]]
        num-src  [[1 2] [3 4]]]
    (fact?<- "Each tuple in source is a vector; the sum of each vector
             should be reflected in the output."
             [[10] [15]]
             [?sum]
             (coll-src ?coll)
             (reduce #'+ ?coll :> ?sum))

    (fact?- "Operation parameters can be vars or anything kryo
             serializable."
            [[1 2 2] [3 4 12]]
            (<- [?x ?y ?z]
                (num-src ?x ?y)
                (var-apply [#'*] ?x ?y :> ?z))

            "Regexes are serializable w/ Kryo."
            [["a" "b"]]
            (<- [?a ?b]   
                ([["a\tb"]] ?s)
                (c/re-parse [#"[^\s]+"] :< ?s :> ?a ?b)))))

(def bob-set #{"bob"})

(deftest test-ifn-implementers
  (let [people [["bob"] ["sam"]]]
    (fact?<- "A set can be used as a predicate op, provided it's bound
             to a var."
             [["bob"]]
             [?person]
             (people ?person)
             (bob-set ?person))))

(deftest test-sample-count
  (let [numbers [[1] [2] [3] [4] [5] [6] [7] [8] [9] [10]]
        sampling-query (c/fixed-sample numbers 5)]
    (fact?<- "sample should return a number of samples equal to the specified
             sample size param"
             [[5]]
             [?count]
             (sampling-query ?s)
             (c/count ?count))))

(deftest test-sample-contents
  (let [numbers [[1 2] [3 4] [5 6] [7 8] [9 10]]
        sampling-query (c/fixed-sample numbers 5)]
    (fact?- "sample should contain some of the inputs"
             (contains #{[1 2] [3 4] [5 6]} :gaps-ok) sampling-query)))

(future-fact "test outer join with functions.")

(future-fact "test mongo"
  "function required for join"
  "2 inner join, 2 outer join portion"
  "functions -> joins -> functions -> joins"
  "functions that run after outer join"
  "no aggregator")

(future-fact "Test only complex aggregators.")
(future-fact "Test only non-complex aggregators.")
(future-fact "test only one buffer.")
(future-fact "Test error on missing output fields.")
