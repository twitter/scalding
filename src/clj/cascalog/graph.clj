(ns cascalog.graph
  (:import [cascalog SimplePrintDirectedGraph]
           [org.jgrapht EdgeFactory]))

(defstruct edge :source :target ::extra-data)
(defstruct node ::graph ::value ::extra-data)

(defn get-extra-data [obj kw]
  (@(::extra-data obj) kw))

(defn add-extra-data [obj kw val]
  (swap! (::extra-data obj) assoc kw val))

(defn update-extra-data [obj kw afn]
  (swap! (::extra-data obj)
         (fn [curr]
           (assoc curr kw (afn (curr kw))))))

(defn mk-graph []
  (SimplePrintDirectedGraph.
   (reify EdgeFactory
     (createEdge [this source target]
       (struct edge source target (atom {}))))))

(defn create-node [graph value]
  (let [ret (struct node graph value (atom {}))]
    (.addVertex graph ret)
    ret))

(defn create-edge [node1 node2]
  (.addEdge (::graph node1) node1 node2))

(defn connect-value
  "Creates a node for val and creates an edge from node -> new node. Returns new node"
  [node val]
  (let [n2 (create-node (::graph node) val)]
    (create-edge node n2)
    n2))

(defn get-graph [node]
  (::graph node))

(defn get-value [node]
  (::value node))

(defn get-outbound-edges [node]
  (or (seq (.outgoingEdgesOf (::graph node) node))
      []))

(defn get-inbound-edges [node]
  (or (seq (.incomingEdgesOf (::graph node) node))
      []))

(defn get-outbound-nodes [node]
  (map :target (get-outbound-edges node)))

(defn get-inbound-nodes [node]
  (map :source (get-inbound-edges node)))

