(ns cascalog.graph-test
  (:use cascalog.graph
        midje.sweet))

(set! *print-level* 3)

(defn just-nodevals [vals]
  (let [check (just vals :in-any-order)]
    (chatty-checker
     [n]
     (check (map get-value (get-outbound-nodes n))))))

(fact "Test adding nodes and edges."
  (let [g (mk-graph)
        n1 (create-node g "AAA")
        n2 (create-node g "BBB")
        n3 (create-node g "CCC")
        n4 (create-node g "DDD")]
    (create-edge n1 n2)
    (create-edge n1 n3)
    (create-edge n3 n4)
    (facts "Check specific values"
      (get-value n1) => "AAA"
      (get-value n3) => "CCC")
    (facts "Check specific nodes."
      n1 => (just-nodevals ["BBB" "CCC"])
      n2 => (just-nodevals [])
      n3 => (just-nodevals ["DDD"])
      n4 => (just-nodevals []))))

(fact "test extra data"
  (let [g  (mk-graph)
        n1 (create-node g "n1")
        n2 (create-node g "n2")
        e  (create-edge n1 n2)]
    (add-extra-data e :a 1)
    (add-extra-data n1 :a 2)
    (add-extra-data n1 :b 5)
    (add-extra-data n2 :b 3)
    (facts "data checks."
      (get-extra-data n1 :a) => 2
      (get-extra-data n1 :b) => 5
      (get-extra-data n1 :c) => nil?
      (get-extra-data e :a) => 1)

    (add-extra-data e :a 101)
    (update-extra-data n2 :b inc)
    (facts "More checks."
      (get-extra-data n2 :b) => 4
      (-> (first (get-outbound-edges n1))
          (get-extra-data :a)) => 101)))
