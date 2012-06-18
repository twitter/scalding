(ns cascalog.rules
  (:use [cascalog.debug :only (debug-print)]
        [clojure.set :only (intersection union difference subset?)]
        [clojure.walk :only (postwalk)]
        [jackknife.core :only (throw-illegal throw-runtime)])
  (:require [cascalog.workflow :as w]
            [jackknife.seq :as s]
            [cascalog.vars :as v]
            [cascalog.util :as u]
            [cascalog.graph :as g]
            [cascalog.predicate :as p]
            [hadoop-util.core :as hadoop]
            [cascalog.conf :as conf])
  (:import [cascading.tap Tap]
           [cascading.tuple Fields Tuple TupleEntry]
           [cascading.flow Flow FlowConnector]
           [cascading.pipe Pipe]
           [cascading.flow.hadoop HadoopFlowProcess]
           [cascading.pipe.joiner CascalogJoiner CascalogJoiner$JoinType]
           [cascalog CombinerSpec ClojureCombiner ClojureCombinedAggregator Util
            ClojureParallelAgg]
           [org.apache.hadoop.mapred JobConf]
           [jcascalog Predicate Subquery PredicateMacro ClojureOp]
           [java.util ArrayList]))

;; infields for a join are the names of the join fields
(p/defpredicate join :infields)
(p/defpredicate group :assembly :infields :totaloutfields)

(defn- find-generator-join-set-vars [node]
  (let [pred (g/get-value node)
        inbound-nodes (g/get-inbound-nodes node)]
    (cond (#{:join :group} (:type pred)) nil
          (= :generator (:type pred)) (if-let [v (:join-set-var pred)] [v])
          :else (do
                  (if-not (= 1 (count inbound-nodes))
                    (throw-runtime (str "Planner exception: Unexpected number of "
                                        "inbound nodes to non-generator predicate."))
                    (recur (first inbound-nodes)))))))

(defn- split-predicates
  "returns [generators operations aggregators]."
  [predicates]
  (let [{ops :operation
         aggs :aggregator
         gens :generator} (merge {:operation [] :aggregator [] :generator []}
                                 (group-by :type predicates))]
    (when (and (> (count aggs) 1)
               (some :buffer? aggs))
      (throw-illegal "Cannot use both aggregators and buffers in same grouping"))
    [gens ops aggs]))

(defstruct tailstruct :ground? :operations :drift-map :available-fields :node)

(defn- connect-op [tail op]
  (let [new-node (g/connect-value (:node tail) op)
        new-outfields (concat (:available-fields tail) (:outfields op))]
    (struct tailstruct
            (:ground? tail)
            (:operations tail)
            (:drift-map tail)
            new-outfields new-node)))

(defn- add-op [tail op]
  (debug-print "Adding op to tail " op tail)
  (let [tail (connect-op tail op)
        new-ops (s/remove-first (partial = op) (:operations tail))]
    (merge tail {:operations new-ops})))

(defn- op-allowed? [tail op]
  (let [ground? (:ground? tail)
        available-fields (:available-fields tail)
        join-set-vars (find-generator-join-set-vars (:node tail))
        infields-set (set (:infields op))]
    (and (or (:allow-on-genfilter? op) (empty? join-set-vars))
         (subset? infields-set (set available-fields))
         (or ground? (every? v/ground-var? infields-set)))))

(defn- fixed-point-operations
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
  (if-let [op (s/find-first (partial op-allowed? tail)
                            (:operations tail))]
    (recur (add-op tail op))
    tail))

;; TODO: refactor and simplify drift algorithm
(defn- add-drift-op [tail equality-sets rename-map new-drift-map]
  (let [eq-assemblies (map w/equal equality-sets)
        outfields (vec (keys rename-map))
        rename-in (vec (vals rename-map))
        rename-assembly (if (seq rename-in)
                          (w/identity rename-in :fn> outfields :> Fields/SWAP)
                          identity)
        assembly   (apply w/compose-straight-assemblies
                          (concat eq-assemblies [rename-assembly]))
        infields (vec (apply concat rename-in equality-sets))
        tail (connect-op tail (p/predicate p/operation assembly infields outfields false))
        newout (difference (set (:available-fields tail)) (set rename-in))]
    (merge tail {:drift-map new-drift-map :available-fields newout} )))

(defn- determine-drift [drift-map available-fields]
  (let [available-set (set available-fields)
        rename-map (reduce (fn [m f]
                             (let [drift (drift-map f)]
                               (if (and drift (not (available-set drift)))
                                 (assoc m drift f)
                                 m)))
                           {} available-fields)
        eqmap (select-keys (u/reverse-map (select-keys drift-map available-fields))
                           available-fields)
        equality-sets (map (fn [[k v]] (conj v k)) eqmap)
        new-drift-map (->> equality-sets
                           (apply concat (vals rename-map))
                           (apply dissoc drift-map))]
    [new-drift-map equality-sets rename-map]))

(defn- add-ops-fixed-point
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
  (let [{:keys [drift-map available-fields] :as tail} (fixed-point-operations tail)
        [new-drift-map equality-sets rename-map] (determine-drift drift-map available-fields)]
    (if (and (empty? equality-sets)
             (empty? rename-map))
      tail
      (recur (add-drift-op tail equality-sets rename-map new-drift-map)))))

(defn- tail-fields-intersection [& tails]
  (->> tails
       (map #(set (:available-fields %)))
       (apply intersection)))

(defn- joinable? [joinfields tail]
  (let [join-set (set joinfields)
        tailfields (set (:available-fields tail))]
    (and (subset? join-set tailfields)
         (or (:ground? tail)
             (every? v/unground-var? (difference tailfields join-set))))))

(defn- find-join-fields [tail1 tail2]
  (let [join-set (tail-fields-intersection tail1 tail2)]
    (when (every? (partial joinable? join-set) [tail1 tail2])
      join-set)))

(defn- select-join
  "Splits tails into [join-fields {join set} {rest of tails}] This is
   unoptimal. It's better to rewrite this as a search problem to find
   optimal joins"
  [tails]
  (let [max-join (->> (u/all-pairs tails)
                      (map (fn [[t1 t2]]
                             (or (find-join-fields t1 t2) [])))
                      (sort-by count)
                      (last))]
    (if (empty? max-join)
      (throw-illegal "Unable to join predicates together")
      (cons (vec max-join)
            (s/separate (partial joinable? max-join) tails)))))

(defn- intersect-drift-maps [drift-maps]
  (let [tokeep (->> drift-maps
                    (map #(set (seq %)))
                    (apply intersection))]
    (u/pairs->map (seq tokeep))))

(defn- select-selector [seq1 selector]
  (mapcat (fn [o b] (if b [o])) seq1 selector))

(defn- merge-tails [graph tails]
  (let [tails (map add-ops-fixed-point tails)]
    (if (= 1 (count tails))
      (add-ops-fixed-point (merge (first tails) {:ground? true})) ; if still unground, allow operations to be applied
      (let [[join-fields join-set rest-tails] (select-join tails)
            join-node             (g/create-node graph (p/predicate join join-fields))
            join-set-vars    (map find-generator-join-set-vars (map :node join-set))
            available-fields (vec (set (apply concat
                                              (cons (apply concat join-set-vars)
                                                    (select-selector
                                                     (map :available-fields join-set)
                                                     (map not join-set-vars))
                                                    ))))
            new-ops          (vec (apply intersection (map #(set (:operations %)) join-set)))
            new-drift-map    (intersect-drift-maps (map :drift-map join-set))]
        (debug-print "Selected join" join-fields join-set)
        (debug-print "Join-set-vars" join-set-vars)
        (debug-print "Available fields" available-fields)
        (dorun (map #(g/create-edge (:node %) join-node) join-set))
        (recur graph (cons (struct tailstruct
                                   (s/some? :ground? join-set)
                                   new-ops
                                   new-drift-map
                                   available-fields
                                   join-node)
                           rest-tails))))))

(defn- agg-available-fields [grouping-fields aggs]
  (vec (union (set grouping-fields) (apply union (map #(set (:outfields %)) aggs)))))

(defn- agg-infields [sort-fields aggs]
  (vec (apply union (set sort-fields) (map #(set (:infields %)) aggs))))

(defn- normalize-grouping
  "Returns [new-grouping-fields inserter-assembly]"
  [grouping-fields]
  (if (seq grouping-fields)
    [grouping-fields identity]
    (let [newvar (v/gen-nullable-var)]
      [[newvar] (w/insert newvar 1)])))

(defn- mk-combined-aggregator [pagg argfields outfields]
  (w/raw-every (w/fields argfields)
               (ClojureCombinedAggregator. (w/fields outfields)
                                           pagg)
               Fields/ALL))

(defn mk-agg-arg-fields [fields]
  (when (seq fields)
    (w/fields fields)))

(defn- mk-parallel-aggregator [grouping-fields aggs]
  (let [argfields  (map #(mk-agg-arg-fields (:infields %)) aggs)
        tempfields (map #(v/gen-nullable-vars (count (:outfields %))) aggs)
        specs      (map :parallel-agg aggs)
        combiner (ClojureCombiner. (w/fields grouping-fields)
                                   argfields
                                   (w/fields (apply concat tempfields))
                                   specs)]
    [[(w/raw-each Fields/ALL combiner Fields/RESULTS)]
     (map mk-combined-aggregator specs tempfields (map :outfields aggs))] ))

(defn- mk-parallel-buffer-agg [grouping-fields agg]
  [[((:parallel-agg agg) grouping-fields)]
   [(:serial-agg-assembly agg)]] )

(defn- build-agg-assemblies
  "returns [pregroup vec, postgroup vec]"
  [grouping-fields aggs]
  (cond (and (= 1 (count aggs))
             (:parallel-agg (first aggs))
             (:buffer? (first aggs)))     (mk-parallel-buffer-agg grouping-fields (first aggs))
             (every? :parallel-agg aggs)  (mk-parallel-aggregator grouping-fields aggs)
             :else                        [[identity] (map :serial-agg-assembly aggs)]))

(defn- mk-group-by [grouping-fields options]
  (let [{s :sort rev :reverse} options]
    (if (seq s)
      (w/group-by grouping-fields s rev)
      (w/group-by grouping-fields))))

(defn- build-agg-tail [options prev-tail grouping-fields aggs]
  (debug-print "Adding aggregators to tail" options prev-tail grouping-fields aggs)
  (when (and (empty? aggs) (:sort options))
    (throw-illegal "Cannot specify a sort when there are no aggregators"))
  (if (and (not (:distinct options))
           (empty? aggs))
    prev-tail  
    (let [aggs (or (not-empty aggs) [p/distinct-aggregator])      
          [grouping-fields inserter] (normalize-grouping grouping-fields)
          [prep-aggs postgroup-aggs] (build-agg-assemblies grouping-fields aggs)
          assem        (apply w/compose-straight-assemblies
                              (concat [inserter]
                                      (map :pregroup-assembly aggs)
                                      prep-aggs
                                      [(mk-group-by grouping-fields options)]
                                      postgroup-aggs
                                      (map :post-assembly aggs)))
          total-fields (agg-available-fields grouping-fields aggs)
          all-agg-infields  (agg-infields (:sort options) aggs)
          prev-node    (:node prev-tail)
          node         (g/create-node (g/get-graph prev-node)
                                      (p/predicate group assem
                                                   all-agg-infields total-fields))]
      (g/create-edge prev-node node)
      (struct tailstruct (:ground? prev-tail) (:operations prev-tail)
              (:drift-map prev-tail) total-fields node))))

(defn projection-fields [needed-vars allfields]
  (let [needed-set (set needed-vars)
        all-set    (set allfields)
        inter      (intersection needed-set all-set)]
    (cond
     ;; maintain ordering when =, this is for output of generators to
     ;; match declared ordering
     (= inter needed-set) needed-vars

     ;; this happens during global aggregation, need to keep one field
     ;; in
     (empty? inter) [(first allfields)]
     :else (vec inter))))

(defn- mk-projection-assembly
  [forceproject projection-fields allfields]
  (if (and (not forceproject) (= (set projection-fields) (set allfields)))
    identity
    (w/select projection-fields)))

(defmulti node->generator (fn [pred & rest] (:type pred)))

(defmethod node->generator :generator [pred prevgens]
  (when (not-empty prevgens)
    (throw (RuntimeException. "Planner exception: Generator has inbound nodes")))
  pred)

(w/defmapop [join-fields-selector [num-fields]] [& args]
  (let [joins (partition num-fields args)]
    (or (s/find-first (partial s/some? (complement nil?)) joins)
        (repeat num-fields nil))))

(w/defmapop truthy? [arg]
  (if arg true false))

(defn- replace-join-fields [join-fields join-renames fields]
  (let [replace-map (zipmap join-fields join-renames)]
    (reduce (fn [ret f]
              (let [newf (replace-map f)
                    newf (if newf newf f)]
                (conj ret newf)))
            [] fields)))

(defn- generate-join-fields [numfields numpipes]
  (take numpipes (repeatedly (partial v/gen-nullable-vars numfields))))

(defn- new-pipe-name [prevgens]
  (.getName (:pipe (first prevgens))))

(defn- gen-join-type [gen]
  (cond (:join-set-var gen) :outerone
        (:ground? gen)      :inner
        :else               :outer))

;; split into inner-gens outer-gens join-set-gens
(defmethod node->generator :join [pred prevgens]
  (debug-print "Creating join" pred)
  (debug-print "Joining" prevgens)
  (let [join-fields (:infields pred)
        num-join-fields (count join-fields)
        sourcemap   (apply merge (map :sourcemap prevgens))
        trapmap     (apply merge (map :trapmap prevgens))
        {inner-gens :inner
         outer-gens :outer
         outerone-gens :outerone} (group-by gen-join-type prevgens)
        join-set-fields (map :join-set-var outerone-gens)
        prevgens    (concat inner-gens outer-gens outerone-gens) ; put them in order
        infields    (map :outfields prevgens)
        inpipes     (map (fn [p f] (w/assemble p (w/select f) (w/pipe-rename (u/uuid))))
                         (map :pipe prevgens)
                         infields) ; is this necessary?
        join-renames (generate-join-fields num-join-fields (count prevgens))
        rename-fields (flatten (map (partial replace-join-fields join-fields) join-renames infields))
        keep-fields (vec (set (apply concat
                                     (cons join-set-fields
                                           (map :outfields
                                                (concat inner-gens outer-gens))))))
        cascalogjoin (concat (repeat (count inner-gens) CascalogJoiner$JoinType/INNER)
                             (repeat (count outer-gens) CascalogJoiner$JoinType/OUTER)
                             (repeat (count outerone-gens) CascalogJoiner$JoinType/EXISTS))
        joined      (apply w/assemble inpipes
                           (concat
                            [(w/co-group (repeat (count inpipes) join-fields)
                                         rename-fields (CascalogJoiner. cascalogjoin))]
                            (mapcat
                             (fn [gen joinfields]
                               (if-let [join-set-var (:join-set-var gen)]
                                 [(truthy? (take 1 joinfields) :fn> [join-set-var] :> Fields/ALL)]))
                             prevgens join-renames)
                            [(join-fields-selector [num-join-fields]
                                                   (flatten join-renames) :fn> join-fields :> Fields/SWAP)
                             (w/select keep-fields)
                             ;; maintain the pipe name (important for setting traps on subqueries)
                             (w/pipe-rename (new-pipe-name prevgens))]))]
    (p/predicate p/generator nil true sourcemap joined keep-fields trapmap)))

(defmethod node->generator :operation [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: operation has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (concat (:outfields pred) (:outfields prevpred))
                     :pipe ((:assembly pred) (:pipe prevpred))})))

(defmethod node->generator :group [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: group has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (:totaloutfields pred)
                     :pipe ((:assembly pred) (:pipe prevpred))})))

;; forceproject necessary b/c *must* reorder last set of fields coming out to match declared ordering
(defn build-generator [forceproject needed-vars node]
  (let [pred           (g/get-value node)
        my-needed      (vec (set (concat (:infields pred) needed-vars)))
        prev-gens      (doall (map (partial build-generator false my-needed)
                                   (g/get-inbound-nodes node)))
        newgen         (node->generator pred prev-gens)
        project-fields (projection-fields needed-vars (:outfields newgen)) ]
    (debug-print "build gen:" my-needed project-fields pred)
    (if (and forceproject (not= project-fields needed-vars))
      (throw-runtime (str "Only able to build to " project-fields
                          " but need " needed-vars))
      (merge newgen
             {:pipe ((mk-projection-assembly forceproject
                                             project-fields
                                             (:outfields newgen))
                     (:pipe newgen))
              :outfields project-fields}))))

(def DEFAULT-OPTIONS
  {:distinct false
   :sort nil
   :reverse false
   :trap nil})

(defn- validate-option-merge! [val-old val-new]
  (if (and (not (nil? val-old)) (not= val-old val-new))
    (throw-runtime "Same option set to conflicting values!")
    val-new))

(defn- mk-options [opt-predicates]
  (->> opt-predicates
       (map (fn [{k :key, v :val}]
              (let [v (if (= :sort k) v (first v))
                    v (if (= :trap k) {:tap v :name (u/uuid)} v)]
                (if (contains? DEFAULT-OPTIONS k)
                  {k v}
                  (throw-illegal (str k " is not a valid option predicate"))))))
       (apply merge-with validate-option-merge!)
       (merge DEFAULT-OPTIONS)))

(defn- mk-var-uniquer-reducer [out?]
  (fn [[preds vmap] [op hof-args invars outvars]]
    (let [[updatevars vmap] (v/uniquify-vars (if out? outvars invars) out? vmap)
          [invars outvars] (if out? [invars updatevars] [updatevars outvars])]
      [(conj preds [op hof-args invars outvars]) vmap])))

;; TODO: Move mk-drift-map to graph?

(defn- uniquify-query-vars
  "TODO: this won't handle drift for generator filter outvars should
  fix this by rewriting and simplifying how drift implementation
  works."
  [out-vars raw-predicates]
  (let [[raw-predicates vmap] (reduce (mk-var-uniquer-reducer true) [[] {}] raw-predicates)
        [raw-predicates vmap] (reduce (mk-var-uniquer-reducer false) [[] vmap] raw-predicates)
        [out-vars vmap]       (v/uniquify-vars out-vars false vmap)
        drift-map             (v/mk-drift-map vmap)]
    [out-vars raw-predicates drift-map]))

(defn split-outvar-constants
  [[op hof-args invars outvars]]
  (let [[new-outvars newpreds] (reduce
                                (fn [[outvars preds] v]
                                  (if (v/cascalog-var? v)
                                    [(conj outvars v) preds]
                                    (let [newvar (v/gen-nullable-var)]
                                      [(conj outvars newvar)
                                       (conj preds [(p/predicate p/outconstant-equal)
                                                    nil [v newvar] []])])))
                                [[] []]
                                outvars)]
    (cons [op hof-args invars new-outvars] newpreds)))

(defn- rewrite-predicate [[op hof-args invars outvars :as predicate]]
  (if-not (and (p/generator? op) (seq invars))
    predicate
    (if (= 1 (count outvars))
      [(p/predicate p/generator-filter op (first outvars)) hof-args [] invars]
      (throw-illegal (str "Generator filter can only have one outvar -> "
                          outvars)))))

(defn- parse-predicate [[op vars]]
  (let [[vars hof-args] (if (p/hof-predicate? op)
                          [(rest vars) (s/collectify (first vars))]
                          [vars nil])
        {invars :<< outvars :>>} (p/parse-variables vars (p/predicate-default-var op))]
    [op hof-args invars outvars]))

(defn- unzip-generators
  "Returns a vector containing two sequences; the subset of the
  supplied sequence of parsed-predicates identified as generators, and
  the rest."
  [parsed-preds]
  (s/separate (comp p/generator? first) parsed-preds))

(defn gen-as-set?
  "Returns true if the supplied parsed predicate is a generator meant
  to be used as a set, false otherwise."
  [parsed-pred]
  (and (p/generator? (first parsed-pred))
       (not-empty (nth parsed-pred 2))))

(defn- gen-as-set-ungrounding-vars
  "Returns a sequence of ungrounding vars present in the
  generators-as-sets contained within the supplied sequence of parsed
  predicates (of the form `[op hof-args invars outvars]`)."
  [parsed-preds]
  (mapcat (comp (partial filter v/unground-var?)
                #(->> % (take-last 2) (apply concat)))
          (filter gen-as-set? parsed-preds)))

(defn- parse-ungrounding-outvars
  "For the supplied sequence of parsed cascalog predicates of the form
  `[op hof-args invars outvars]`, returns a vector of two
  entries: a sequence of all output ungrounding vars that appear
  within generator predicates, and a sequence of all ungrounding vars
  that appear within non-generator predicates."
  [parsed-preds]
  (map (comp (partial mapcat #(filter v/unground-var? %))
             (partial map last))
       (unzip-generators parsed-preds)))

(defn- pred-clean!
  "Performs various validations on the supplied set of parsed
  predicates. If all validations pass, returns the sequence
  unchanged."
  [parsed-preds]
  (let [gen-as-set-vars (gen-as-set-ungrounding-vars parsed-preds)
        [gen-outvars pred-outvars] (parse-ungrounding-outvars parsed-preds)
        extra-vars  (vec (difference (set pred-outvars)
                                     (set gen-outvars)))
        dups (s/duplicates gen-outvars)]
    (cond
     (not-empty gen-as-set-vars)
     (throw-illegal (str "Can't have unground vars in generators-as-sets."
                         (vec gen-as-set-vars)
                         " violate(s) the rules.\n\n" (pr-str parsed-preds)))

     (not-empty extra-vars)
     (throw-illegal (str "Ungrounding vars must originate within a generator. "
                         extra-vars
                         " violate(s) the rules."))

     (not-empty dups)
     (throw-illegal (str "Each ungrounding var can only appear once per query."
                         "The following are duplicated: "
                         dups))
     :else parsed-preds)))

(defn- build-query [out-vars raw-predicates]
  (debug-print "outvars:" out-vars)
  (debug-print "raw predicates:" raw-predicates)
  (let [[out-vars raw-predicates drift-map] (->> raw-predicates
                                                 (map parse-predicate)
                                                 (pred-clean!)
                                                 (mapcat split-outvar-constants)
                                                 (map rewrite-predicate)
                                                 (mapcat split-outvar-constants)
                                                 (uniquify-query-vars out-vars))
        [raw-opts raw-predicates] (s/separate #(keyword? (first %)) raw-predicates)
        options                   (mk-options (map p/mk-option-predicate raw-opts))
        [gens ops aggs]           (->> raw-predicates
                                       (map (partial apply p/build-predicate options))
                                       (split-predicates))
        rule-graph                (g/mk-graph)
        joined                    (->> gens
                                       (map (fn [{:keys [ground? outfields] :as g}]
                                              (struct tailstruct ground?
                                                      ops drift-map outfields
                                                      (g/create-node rule-graph g))))
                                       (merge-tails rule-graph))
        grouping-fields           (seq (intersection (set (:available-fields joined))
                                                     (set out-vars)))
        agg-tail                  (build-agg-tail options joined grouping-fields aggs)
        {:keys [operations node]} (add-ops-fixed-point agg-tail)]
    (if (not-empty operations)
      (throw-runtime (str "Could not apply all operations " (pr-str operations)))
      (build-generator true out-vars node))))

(defn- new-var-name! [replacements v]
  (let [new-name  (if (contains? @replacements v)
                    (@replacements v)
                    (v/uniquify-var v))]
    (swap! replacements assoc v new-name)
    new-name))

(defn- pred-macro-updater [[replacements ret] [op vars]]
  (let [newvars (postwalk #(if (v/cascalog-var? %)
                             (new-var-name! replacements %)
                             %)
                          vars)]
    [replacements (conj ret [op newvars])]))

(defn collectify-nil-as-seq [v]
  (if v (s/collectify v)))

(defn- build-predicate-macro-fn [invars-decl outvars-decl raw-predicates]
  (when (seq (intersection (set (collectify-nil-as-seq invars-decl))
                           (set (collectify-nil-as-seq outvars-decl))))
    (throw
     (RuntimeException.
      (str "Cannot declare the same var as an input and output to predicate macro: "
            invars-decl " " outvars-decl))))
    (fn [invars outvars]
      (let [outvars (if (and (empty? outvars)
                             (sequential? outvars-decl)
                             (= 1 (count outvars-decl)))
                      [true]
                      outvars) ; kind of a hack, simulate using pred macros like filters
            replacements (atom (u/mk-destructured-seq-map invars-decl
                                                          invars
                                                          outvars-decl
                                                          outvars))]
        (second (reduce pred-macro-updater [replacements []] raw-predicates)))))

(defn- build-predicate-macro [invars outvars raw-predicates]
  (p/predicate p/predicate-macro
               (build-predicate-macro-fn invars outvars raw-predicates)))

(defn- to-jcascalog-fields [fields]
  (if fields
    (jcascalog.Fields. fields)
    (jcascalog.Fields. [])
    ))

(defn- expand-predicate-macro
  [p vars]
  (let [{invars :<< outvars :>>} (p/parse-variables vars :<)]
    (cond (var? p)
          [[(var-get p) vars]]
      
          (instance? Subquery p)
          [[(.getCompiledSubquery p) vars]]
        
          (instance? ClojureOp p)
          [(.toRawCascalogPredicate p vars)]
        
          (instance? PredicateMacro p)
          (.getPredicates p (to-jcascalog-fields invars) (to-jcascalog-fields outvars))

          :else
          ((:pred-fn p) invars outvars))))

(defn- expand-predicate-macros [raw-predicates]
  (mapcat (fn [raw-predicate]
            (let [[p vars :as raw-predicate]
                             (if (instance? Predicate raw-predicate)
                                (.toRawCascalogPredicate raw-predicate)
                                raw-predicate )]
              (if (p/predicate-macro? p)
                (expand-predicate-macros (expand-predicate-macro p vars))
                [raw-predicate])))
          raw-predicates))

(defn build-rule [out-vars raw-predicates]
  (let [raw-predicates (-> raw-predicates
                           expand-predicate-macros)
        parsed (p/parse-variables out-vars :?)]
    (if (seq (parsed :?))
      (build-query out-vars raw-predicates)
      (build-predicate-macro (parsed :<<)
                             (parsed :>>)
                             raw-predicates))))

(defn mk-raw-predicate [[op-sym & vars]]
  [op-sym (v/vars->str vars)])

(defn pluck-tuple [tap]
  (with-open [it (-> (HadoopFlowProcess. (hadoop/job-conf (conf/project-conf)))
                     (.openTapForRead tap))]
    (if-let [iter (iterator-seq it)]
      (-> iter first .getTuple Tuple. Util/coerceFromTuple vec)
      (throw-illegal "Cascading tap is empty -- tap must contain tuples."))))

(defn enforce-gen-schema
  "Accepts a cascalog generator; if `g` is well-formed, acts as
`identity`. If the supplied generator is a vector, list, or a straight
cascading tap, returns a new generator with field-names."
  [g]
  (cond (= (:type g) :cascalog-tap)
        (enforce-gen-schema (:source g))

        (or (instance? Tap g)
            (vector? g)
            (list? g))
        (let [pluck (if (instance? Tap g) pluck-tuple, first)
              size  (count (pluck g))
              vars  (v/gen-nullable-vars size)]
          (if (zero? size)
            (throw-illegal
             "Data structure is empty -- memory sources must contain tuples.")
            (->> [[g :>> vars] [:distinct false]]
                 (map mk-raw-predicate)
                 (build-rule vars))))
        :else g))

;; TODO: Why does this not use gen?
(defn connect-to-sink [gen sink]
  ((w/pipe-rename (u/uuid)) (:pipe gen)))

(defn normalize-gen [gen]
  (if (instance? Subquery gen)
    (.getCompiledSubquery gen)
    gen))

(defn combine* [gens distinct?]
  ;; it would be nice if cascalog supported Fields/UNKNOWN as output of generator
  (let [gens (->> gens (map normalize-gen) (map enforce-gen-schema))
        outfields (:outfields (first gens))
        pipes (map :pipe gens)
        pipes (for [p pipes]
                (w/assemble p (w/identity Fields/ALL :fn> outfields :> Fields/RESULTS)))
        outpipe (if-not distinct?
                  (w/assemble pipes (w/group-by Fields/ALL))
                  (w/assemble pipes (w/group-by Fields/ALL) (w/first)))]
    (p/predicate p/generator
                 nil
                 true
                 (apply merge (map :sourcemap gens))
                 outpipe
                 outfields
                 (apply merge (map :trapmap gens)))))

(defn generic-cascading-fields? [cfields]
  (or (.isSubstitution cfields)
      (.isUnknown cfields)
      (.isResults cfields)
      (.isSwap cfields)
      (.isReplace cfields)))

(defn generator-selector [gen & args]
  (cond (instance? Tap gen) :tap
        (instance? Subquery gen) :java-subquery
        :else (:type gen)))

(defn normalize-sink-connection [sink subquery]
  (cond (fn? sink)  (sink subquery)
        (map? sink) (normalize-sink-connection (:sink sink) subquery)
        :else       [sink subquery]))

(defn parse-exec-args [[f & rest :as args]]
  (if (string? f)
    [f rest]
    ["" args]))

(defn get-sink-tuples [^Tap sink]
  (let [conf (hadoop/job-conf (conf/project-conf))]
    (cond (map? sink)
          (get-sink-tuples (:sink sink))

          (not (.resourceExists sink conf))
          []

          :else (with-open [it (-> (HadoopFlowProcess. conf)
                                   (.openTapForRead sink))]
                  (doall
                   (for [^TupleEntry t (iterator-seq it)]
                     (into [] (Tuple. (.getTuple t)))))))))
