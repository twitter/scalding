(ns cascalog.api
  (:use [cascalog.debug :only (debug-print)]
        [jackknife.core :only (safe-assert throw-runtime)]
        [jackknife.def :only (defalias)]
        [jackknife.seq :only (unweave collectify)])
  (:require [clojure.set :as set]
            [cascalog.vars :as v]
            [cascalog.tap :as tap]
            [cascalog.conf :as conf]
            [cascalog.workflow :as w]
            [cascalog.predicate :as p]
            [cascalog.rules :as rules]
            [cascalog.io :as io]
            [cascalog.util :as u]
            [hadoop-util.core :as hadoop])  
  (:import [cascading.flow Flow FlowDef]
           [cascading.flow.hadoop HadoopFlowConnector]
           [cascading.tuple Fields]
           [com.twitter.maple.tap MemorySourceTap StdoutTap]
           [cascading.pipe Pipe]))

;; Functions for creating taps and tap helpers

(defalias memory-source-tap w/memory-source-tap)
(defalias cascalog-tap tap/mk-cascalog-tap)
(defalias hfs-tap tap/hfs-tap)
(defalias lfs-tap tap/lfs-tap)
(defalias sequence-file w/sequence-file)
(defalias text-line w/text-line)

(defn hfs-textline
  "Creates a tap on HDFS using textline format. Different filesystems
   can be selected by using different prefixes for `path`.

  Supports keyword option for `:outfields`. See `cascalog.tap/hfs-tap`
  for more keyword arguments.
   
   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path & opts]
  (let [scheme (->> (:outfields (apply array-map opts) Fields/ALL)
                    (w/text-line ["line"]))]
    (apply tap/hfs-tap scheme path opts)))

(defn lfs-textline
  "Creates a tap on the local filesystem using textline format.

  Supports keyword option for `:outfields`. See `cascalog.tap/lfs-tap`
  for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path & opts]
  (let [scheme (->> (:outfields (apply array-map opts) Fields/ALL)
                    (w/text-line ["line"]))]
    (apply tap/lfs-tap scheme path opts)))

(defn hfs-seqfile
  "Creates a tap on HDFS using sequence file format. Different
   filesystems can be selected by using different prefixes for `path`.

  Supports keyword option for `:outfields`. See `cascalog.tap/hfs-tap`
  for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (w/sequence-file))]
    (apply tap/hfs-tap scheme path opts)))

(defn lfs-seqfile
  "Creates a tap that reads data off of the local filesystem in
   sequence file format.

  Supports keyword option for `:outfields`. See `cascalog.tap/lfs-tap`
  for more keyword arguments.
   
   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (w/sequence-file))]
    (apply tap/lfs-tap scheme path opts)))

(defn stdout
  "Creates a tap that prints tuples sunk to it to standard
   output. Useful for experimentation in the REPL."
  [] (StdoutTap.))

;; Query introspection

(defmulti get-out-fields
  "Get the fields of a generator."  
  rules/generator-selector)

(defmethod get-out-fields :tap [tap]
  (let [cfields (.getSourceFields tap)]
    (safe-assert (not (rules/generic-cascading-fields? cfields))
                 (str "Cannot get specific out-fields from tap. Tap source fields: "
                      cfields))
    (vec (seq cfields))))

(defmethod get-out-fields :generator [query]
  (:outfields query))

(defmethod get-out-fields :cascalog-tap [cascalog-tap]
  (get-out-fields (:source cascalog-tap)))

(defmethod get-out-fields :java-subquery [sq]
  (get-out-fields (.getCompiledSubquery sq)))

(defn num-out-fields [gen]
  (if (or (list? gen) (vector? gen))
    (count (first gen))
    ;; TODO: should pluck from Tap if it doesn't define out-fields
    (count (get-out-fields gen))))

;; Knobs for Hadoop

(defmacro with-job-conf
  "Modifies the job conf for queries executed within the form. Nested
   with-job-conf calls will merge configuration maps together, with
   innermost calls taking precedence on conflicting keys."
  [conf & body]
  `(binding [conf/*JOB-CONF*
             (u/conf-merge conf/*JOB-CONF* ~conf)]
     ~@body))

(defmacro with-serializations
  "Enables the supplied serializations for queries executed within the
  form. Serializations should be provided as a vector of strings or
  classes, like so:

  (import 'org.apache.hadoop.io.serializer.JavaSerialization)
  (with-serializations [JavaSerialization]
     (?<- ...))

  Serializations nest; nested calls to with-serializations will merge
  and unique with serializations currently specified by other calls to
  `with-serializations` or `with-job-conf`."
  [serial-vec & forms]
  `(with-job-conf 
     {"io.serializations" (u/serialization-entry ~serial-vec)}
     ~@forms))

;; Query creation and execution

(defmacro <-
  "Constructs a query or predicate macro from a list of
  predicates. Predicate macros support destructuring of the input and
  output variables."
  [outvars & predicates]
  (let [predicate-builders (vec (map rules/mk-raw-predicate predicates))
        outvars-str (if (vector? outvars) (v/vars->str outvars) outvars)]
    `(rules/build-rule ~outvars-str ~predicate-builders)))

(def cross-join
  (<- [:>] (identity 1 :> _)))

(defn compile-flow
  "Attaches output taps to some number of subqueries and creates a
  Cascading flow. The flow can be executed with `.complete`, or
  introspection can be done on the flow.

  Syntax: (compile-flow sink1 query1 sink2 query2 ...)
  or (compile-flow flow-name sink1 query1 sink2 query2)
   
   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& args]
  (let [[flow-name bindings] (rules/parse-exec-args args)
        [sinks gens] (->> bindings
                          (map rules/normalize-gen)
                          (partition 2)
                          (mapcat (partial apply rules/normalize-sink-connection))
                          (unweave))
        gens      (map rules/enforce-gen-schema gens)
        sourcemap (apply merge (map :sourcemap gens))
        trapmap   (apply merge (map :trapmap gens))
        tails     (map rules/connect-to-sink gens sinks)
        sinkmap   (w/taps-map tails sinks)
        flowdef   (-> (FlowDef.)
                      (.setName flow-name)
                      (.addSources sourcemap)
                      (.addSinks sinkmap)
                      (.addTraps trapmap)
                      (.addTails (into-array Pipe tails)))]
    (-> (HadoopFlowConnector.
         (u/project-merge (conf/project-conf)
                          {"cascading.flow.job.pollinginterval" 100}))
        (.connect flowdef))))

(defn ?-
  "Executes 1 or more queries and emits the results of each query to
  the associated tap.

  Syntax: (?- sink1 query1 sink2 query2 ...)  or (?- query-name sink1
  query1 sink2 query2)
   
   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& bindings]
  (let [^Flow flow (apply compile-flow bindings)]
    (.complete flow)
    (when-not (-> flow .getFlowStats .isSuccessful)
      (throw-runtime "Flow failed to complete."))))

(defn ??-
  "Executes one or more queries and returns a seq of seqs of tuples
   back, one for each subquery given.
  
  Syntax: (??- sink1 query1 sink2 query2 ...)"
  [& subqueries]
  ;; TODO: should be checking for flow name here
  (io/with-fs-tmp [fs tmp]
    (hadoop/mkdirs fs tmp)
    (let [outtaps (for [q subqueries] (hfs-seqfile (str tmp "/" (u/uuid))))
          bindings (mapcat vector outtaps subqueries)]
      (apply ?- bindings)
      (doall (map rules/get-sink-tuples outtaps)))))

(defmacro ?<-
  "Helper that both defines and executes a query in a single call.
  
  Syntax: (?<- out-tap out-vars & predicates) or (?<- \"myflow\"
  out-tap out-vars & predicates) ; flow name must be a static string
  within the ?<- form."
  [& args]
  ;; This is the best we can do... if want non-static name should just use ?-
  (let [[name [output & body]] (rules/parse-exec-args args)]
    `(?- ~name ~output (<- ~@body))))

(defmacro ??<-
  "Like ??-, but for ?<-. Returns a seq of tuples."
  [& args]
  `(io/with-fs-tmp [fs# tmp1#]
     (let [outtap# (hfs-seqfile tmp1#)]
       (?<- outtap# ~@args)
       (rules/get-sink-tuples outtap#))))

(defn predmacro*
  "Functional version of predmacro. See predmacro for details."
  [pred-macro-fn]
  (p/predicate p/predicate-macro
               (fn [invars outvars]
                 (for [[op & vars] (pred-macro-fn invars outvars)]
                   [op vars]))))

(defmacro predmacro
  "A more general but more verbose way to create predicate macros.

   Creates a function that takes in [invars outvars] and returns a
   list of predicates. When making predicate macros this way, you must
   create intermediate variables with gen-nullable-var(s). This is
   because unlike the (<- [?a :> ?b] ...) way of doing pred macros,
   Cascalog doesn't have a declaration for the inputs/outputs.
   
   See https://github.com/nathanmarz/cascalog/wiki/Predicate-macros
  "
  [& body]
  `(predmacro* (fn ~@body)))

(defn construct
  "Construct a query or predicate macro functionally. When
constructing queries this way, operations should either be vars for
operations or values defined using one of Cascalog's def macros. Vars
must be stringified when passed to construct. If you're using
destructuring in a predicate macro, the & symbol must be stringified
as well."
  [outvars preds]
  (let [outvars (v/vars->str outvars)
        preds (for [[p & vars] preds] [p (v/vars->str vars)])]
    (rules/build-rule outvars preds)))

(defn union
  "Merge the tuples from the subqueries together into a single
  subquery and ensure uniqueness of tuples."
  [& gens]
  (rules/combine* gens true))

(defn combine
  "Merge the tuples from the subqueries together into a single
  subquery. Doesn't ensure uniqueness of tuples."
  [& gens]
  (rules/combine* gens false))

(defn multigroup*
  [declared-group-vars buffer-out-vars buffer-spec & sqs]
  (let [[buffer-op hof-args] (if (sequential? buffer-spec) buffer-spec [buffer-spec nil])
        sq-out-vars (map get-out-fields sqs)
        group-vars (apply set/intersection (map set sq-out-vars))
        num-vars (reduce + (map count sq-out-vars))
        pipes (into-array Pipe (map :pipe sqs))
        args [declared-group-vars :fn> buffer-out-vars]
        args (if hof-args (cons hof-args args) args)]
    (safe-assert (seq declared-group-vars)
                 "Cannot do global grouping with multigroup")
    (safe-assert (= (set group-vars) (set declared-group-vars))
                 "Declared group vars must be same as intersection of vars of all subqueries")
    (p/predicate p/generator nil
                 true
                 (apply merge (map :sourcemap sqs))
                 ((apply buffer-op args) pipes num-vars)
                 (concat declared-group-vars buffer-out-vars)
                 (apply merge (map :trapmap sqs)))))

(defmacro multigroup
  [group-vars out-vars buffer-spec & sqs]
  `(multigroup* ~(v/vars->str group-vars)
                ~(v/vars->str out-vars)
                ~buffer-spec
                ~@sqs))

(defmulti select-fields 
"
  Select fields of a named generator.

  Example:
  (<- [?a ?b ?sum]
      (+ ?a ?b :> ?sum)
      ((select-fields generator [\"?a\" \"?b\"]) ?a ?b))
"
  rules/generator-selector)

(defmethod select-fields :tap [tap fields]
  (let [fields (collectify fields)
        pname (u/uuid)
        outfields (v/gen-nullable-vars (count fields))
        pipe (w/assemble (w/pipe pname) (w/identity fields :fn> outfields :> outfields))]
    (p/predicate p/generator nil true {pname tap} pipe outfields {})))

(defmethod select-fields :generator [query select-fields]
  (let [select-fields (collectify select-fields)
        outfields (:outfields query)]
    (safe-assert (set/subset? (set select-fields) (set outfields))
                 (str "Cannot select " select-fields " from " outfields))
    (merge query
           {:pipe (w/assemble (:pipe query) (w/select select-fields))
            :outfields select-fields})))

(defmethod select-fields :cascalog-tap [cascalog-tap fields]
  (select-fields (:source cascalog-tap) fields))

(defmethod select-fields :java-subquery [sq fields]
  (select-fields (.getCompiledSubquery sq) fields))

(defn name-vars [gen vars]
  (let [vars (collectify vars)]
    (<- vars (gen :>> vars) (:distinct false))))

;; Defining custom operations

(defalias defmapop w/defmapop
  "Defines a custom operation that appends new fields to the input tuple.")

(defalias defmapcatop w/defmapcatop)

(defalias defbufferop w/defbufferop)

(defalias defmultibufferop w/defmultibufferop)

(defalias defbufferiterop w/defbufferiterop)

(defalias defaggregateop w/defaggregateop)

(defalias deffilterop w/deffilterop)

(defalias defparallelagg p/defparallelagg)

(defalias defparallelbuf p/defparallelbuf)

;; Miscellaneous helpers

(defn div
  "Perform floating point division on the arguments. Use this instead
   of / in Cascalog queries since / produces Ratio types which aren't
   serializable by Hadoop."
  [f & rest]
  (apply / (double f) rest))

(defmacro with-debug
  "Wrap queries in this macro to cause debug information for the query
   planner to be printed out."
  [& body]
  `(binding [cascalog.debug/*DEBUG* true]
     ~@body))

;; Class Creation

(defmacro defmain
  "Defines an AOT-compiled function with the supplied
  `name`. Containing namespace must be marked for AOT compilation to
  have any effect."
  [name & forms]
  (let [classname (namespace-munge (str *ns* "." name))
        sym (with-meta
              (symbol (str name "-main"))
              (meta name))]
    `(do (gen-class :name ~classname
                    :main true
                    :prefix ~(str name "-"))
         (defn ~(u/meta-conj sym {:no-doc true
                                  :skip-wiki true})
           ~@forms)
         (defn ~name ~@forms))))
