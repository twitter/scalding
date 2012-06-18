(ns cascalog.testing
  (:use clojure.test
        cascalog.api        
        [jackknife.seq :only (unweave collectify)])
  (:require [cascalog.workflow :as w]
            [cascalog.util :as u]
            [cascalog.io :as io]
            [cascalog.rules :as rules]
            [cascalog.conf :as conf]
            [hadoop-util.core :as hadoop])
  (:import [cascading.tuple Fields Tuple TupleEntry TupleEntryCollector]
           [cascading.pipe Pipe]
           [cascading.operation ConcreteCall]
           [cascading.flow FlowProcess]
           [cascading.flow.hadoop HadoopFlowProcess]
           [cascalog Util ClojureMap]
           [com.twitter.maple.tap MemorySourceTap]
           [java.lang Comparable]
           [java.util ArrayList]
           [clojure.lang IPersistentCollection]
           [org.apache.hadoop.mapred JobConf]
           [cascading.flow.hadoop.util HadoopUtil]
           [java.io File]))

(defn roundtrip [obj]
  (HadoopUtil/deserializeBase64
   (HadoopUtil/serializeBase64 obj)))

(defn invoke-filter [fil coll]
  (let [fil     (roundtrip fil)
        op-call (ConcreteCall.)
        fp-null FlowProcess/NULL]
    (.setArguments op-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare fil fp-null op-call)
    (let [rem (.isRemove fil fp-null op-call)]
      (.cleanup fil fp-null op-call)
      rem)))

(defn- output-collector [out-atom]
  (proxy [TupleEntryCollector] []
    (add [tuple]
      (swap! out-atom conj (Util/coerceFromTuple tuple)))))

(defn- op-call []
  (let [args-atom    (atom nil)
        out-atom     (atom [])
        context-atom (atom nil)]
    (proxy [ConcreteCall IPersistentCollection] []
      (setArguments [tuple]
        (swap! args-atom (constantly tuple)))
      (getArguments []
        @args-atom)
      (getOutputCollector []
        (output-collector out-atom))
      (setContext [context]
        (swap! context-atom (constantly context)))
      (getContext []
        @context-atom)
      (seq []
        (seq @out-atom)))))

(defn- op-call-results [func-call]
  (.seq func-call))

(defn invoke-function [m coll]
  (let [m         (roundtrip m)
        func-call (op-call)
        fp-null   FlowProcess/NULL]
    (.setArguments func-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare m fp-null func-call)
    (.operate m fp-null func-call)
    (.cleanup m fp-null func-call)
    (op-call-results func-call)))

(defn invoke-aggregator [a colls]
  (let [a       (roundtrip a)
        ag-call (op-call)
        fp-null FlowProcess/NULL]
    (.prepare a fp-null ag-call)
    (.start a fp-null ag-call)
    (doseq [coll colls]
      (.setArguments ag-call (TupleEntry. (Util/coerceToTuple coll)))
      (.aggregate a fp-null ag-call))
    (.complete a fp-null ag-call)
    (.cleanup  a fp-null ag-call)
    (op-call-results ag-call)))

(defn mk-test-tap [fields-def path]
  (w/lfs (w/sequence-file fields-def) path))

(defn unique-rooted-paths [root]
  (map str (cycle [(str root "/")]) (repeatedly u/uuid)))

(defn- gen-fake-fields [amt]
  (take amt (map str (iterate inc 1))))

(defn- mapify-spec [spec]
  (if (map? spec)
    spec
    {:fields Fields/ALL :tuples spec}))

(defn mk-test-source [spec path]
  ;; unable to use with-log-level here for some reason
  (let [spec (mapify-spec spec)
        source (mk-test-tap (:fields spec) path)]
    (with-open [^TupleEntryCollector collector (-> (HadoopFlowProcess.
                                                    (hadoop/job-conf
                                                     (conf/project-conf)))
                                                   (.openTapForWrite source))]
      (doall (map #(.add collector (Util/coerceToTuple %))
                  (-> spec mapify-spec :tuples)))
      source)))

(defn mk-test-sink [spec path]
  (mk-test-tap (:fields (mapify-spec spec)) path))

(defn test-assembly
  ([source-specs sink-specs assembly]
     (test-assembly :fatal source-specs sink-specs assembly))
  ([log-level source-specs sink-specs assembly]
     (io/with-log-level log-level
       (io/with-tmp-files [source-path (io/temp-dir "sources")
                           sink-path   (io/temp-path "sinks")]
         (let [source-specs  (collectify source-specs)
               sink-specs     (collectify sink-specs)
               sources        (map mk-test-source
                                   source-specs
                                   (unique-rooted-paths source-path))
               sinks          (map mk-test-sink
                                   sink-specs
                                   (unique-rooted-paths sink-path))
               flow           (w/mk-flow sources sinks assembly)
               _              (w/exec flow)
               out-tuples     (doall (map rules/get-sink-tuples sinks))
               expected-data  (map :tuples sink-specs)]
           (is (= (map u/multi-set expected-data)
                  (map u/multi-set out-tuples))))))))

(defn- mk-tmpfiles+forms [amt]
  (let [tmpfiles  (take amt (repeatedly (fn [] (gensym "tap"))))
        tmpforms  (->> tmpfiles
                       (mapcat (fn [f]
                                 [f `(File.
                                      (str (cascalog.io/temp-dir ~(str f))
                                           "/"
                                           (u/uuid)))])))]
    [tmpfiles (vec tmpforms)]))

;; TODO: should rewrite this to use in memory tap
(defmacro with-tmp-sources
  "bindings are name spec, where spec is either {:fields :tuples} or
  vector of tuples."
  [bindings & body]
  (let [[names specs] (unweave bindings)
        [tmpfiles tmpforms] (mk-tmpfiles+forms (count specs))
        tmptaps   (vec (mapcat (fn [n t s]
                                 [n `(cascalog.testing/mk-test-source ~s ~t)])
                               names tmpfiles specs))]
    `(cascalog.io/with-tmp-files ~tmpforms
       (let ~tmptaps
         ~@body))))

(defn- doublify [tuples]
  (vec (for [t tuples]
         (into [] (map (fn [v] (if (number? v) (double v) v))
                       (collectify t))))))

(defn is-specs= [set1 set2]
  (every? true? (doall
                 (map (fn [input output]
                        (let [input  (u/multi-set (doublify input))
                              output (u/multi-set (doublify output))]
                          (is (= input output))))
                      set1 set2))))

(defn is-tuplesets= [set1 set2]
  (is-specs= [set1] [set2]))

(defn process?-
  "Returns a 2-tuple containing a sequence of the original result
  vectors and a sequence of the output tuples generated by running the
  supplied queries with test settings."
  [& [ll :as bindings]]
  (let [[log-level bindings] (if (contains? io/log-levels ll)
                               [ll (rest bindings)]
                               [:fatal bindings])]
    (io/with-log-level log-level
      (io/with-tmp-files [sink-path (io/temp-dir "sink")]
        (with-job-conf {"io.sort.mb" 10}
          (let [bindings (mapcat (partial apply rules/normalize-sink-connection)
                                 (partition 2 bindings))
                [specs rules]  (unweave bindings)
                sinks          (map mk-test-sink specs (unique-rooted-paths sink-path))
                _              (apply ?- (interleave sinks rules))
                out-tuples     (doall (map rules/get-sink-tuples sinks))]
            [specs out-tuples]))))))

(defn test?- [& bindings]
  (let [[specs out-tuples] (apply process?- bindings)]
    (is-specs= specs out-tuples)))

(defn check-tap-spec [tap spec]
  (is-tuplesets= (rules/get-sink-tuples tap) spec))

(defn check-tap-spec-sets [tap spec]
  (is (= (u/multi-set (map set (doublify (rules/get-sink-tuples tap))))
         (u/multi-set (map set (doublify spec))))))

(defn with-expected-sinks-helper [checker bindings body]
  (let [[names specs] (map vec (unweave bindings))
        [tmpfiles tmpforms] (mk-tmpfiles+forms (count names))
        tmptaps (mapcat (fn [n t s]
                          [n `(cascalog.testing/mk-test-sink ~s ~t)])
                        names tmpfiles specs)]
    `(cascalog.io/with-tmp-files ~tmpforms
       (let [~@tmptaps]
         ~@body
         (dorun (map ~checker ~names ~specs))))))

;; bindings are name spec, where spec is either {:fields :tuples} or
;; vector of tuples
(defmacro with-expected-sinks [bindings & body]
  (with-expected-sinks-helper check-tap-spec bindings body))

(defmacro with-expected-sink-sets [bindings & body]
  (with-expected-sinks-helper check-tap-spec-sets bindings body))

(defmacro test?<- [& args]
  (let [[begin body] (if (keyword? (first args))
                       (split-at 2 args)
                       (split-at 1 args))]
    `(test?- ~@begin (<- ~@body))))

(defmacro thrown?<- [error & body]
  `(is (~'thrown? ~error (<- ~@body))))
