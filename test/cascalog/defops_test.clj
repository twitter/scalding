(ns cascalog.defops-test
  (:use cascalog.api
        clojure.test
        [midje sweet cascalog]))

(defmapop ident [x] x)

(defmapop ident-doc
  "Identity operation."
  [x] x)

(defmapop ident-meta
  {:great-meta "yes!"}
  [x] x)

(defmapop ident-both
  "Identity operation."
  {:great-meta "yes!"}
  [x] x)

(defmapop [ident-stateful [y]]
  "Identity operation."
  {:stateful true
   :great-meta "yes!"}
  ([] 3)
  ([state x] (+ x y state))
  ([state] nil))

(deftest defops-arg-parsing-test
  (let [src      [[1] [2]]
        mk-query (fn [afn]
                   (<- [?y] (src ?x) (afn ?x :> ?y)))]

    "This query should add 3 plus the param to each input var from
    src."
    (fact?<- [[5] [6]]
             [?y]
             (src ?x)
             (ident-stateful [1] ?x :> ?y))
    (tabular
     (fact
       "Each function will be applied to `mk-query` in turn; all of
       these functions act as identity transformations, so each query
       should produce the original source without modification."
       (mk-query ?func) => (produces src))
     ?func
     ident
     ident-doc
     ident-meta
     ident-both)))

(facts "Metadata testing."
  "Both function and var should contain custom metadata."
  (meta ident-stateful) => (contains {:great-meta "yes!"})
  (meta #'ident-stateful) => (contains {:great-meta "yes!"})

  "Both function and var should contain docstrings."
  (meta ident-doc) => (contains {:doc "Identity operation."})
  (meta #'ident-doc) => (contains {:doc "Identity operation."})

  "ident-meta shouldn't have a docstring in its metadata."
  (meta #'ident-meta) =not=> (contains {:doc anything}))


(defn five->two [a b c d e]
  [(+ a b c) (+ d e)])

(defn four->one [a b c d]
  (+ a b c d))

(defparallelagg multi-combine
  :init-var #'five->two
  :combine-var #'four->one)

(deftest agg-test
  (fact "Test of aggregators with multiple arguments."
   (let [src [[1 2 3 4 5] [5 6 7 8 9]]]
     "init-var takes n args, outputs x. combine-var takes 2*x args,
     outputs x."
     (fact?<- [[50]]
              [?sum]
              (src ?a ?b ?c ?d ?e)
              (multi-combine ?a ?b ?c ?d ?e :> ?sum)))))
