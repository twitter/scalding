(ns cascalog.util
  (:use [jackknife.core :only (update-vals)]
        [jackknife.seq :only (unweave merge-to-vec collectify)])
  (:require [clojure.string :as s])
  (:import [java.util UUID]))

(defn multifn? [x]
  (instance? clojure.lang.MultiFn x))

(defn try-update-in
  [m key-vec f & args]
  (reduce #(%2 %1) m
          (for [k key-vec]
            #(if (get % k)
               (apply update-in % [k] f args)
               %))))

(defn substitute-if
  "Returns [newseq {map of newvals to oldvals}]"
  [pred subfn aseq]
  (reduce (fn [[newseq subs] val]
            (let [[newval sub] (if (pred val)
                                 (let [subbed (subfn val)] [subbed {subbed val}])
                                 [val {}])]
              [(conj newseq newval) (merge subs sub)]))
          [[] {}] aseq))

(defn multi-set
  "Returns a map of elem to count"
  [aseq]
  (apply merge-with +
         (map #(hash-map % 1) aseq)))

(defn uuid []
  (str (UUID/randomUUID)))

(defn all-pairs
  "[1 2 3] -> [[1 2] [1 3] [2 3]]"
  [coll]
  (let [pair-up (fn [v vals]
                  (map (partial vector v) vals))]
    (apply concat (for [i (range (dec (count coll)))]
                    (pair-up (nth coll i) (drop (inc i) coll))))))

(defn pairs->map [pairs]
  (apply hash-map (flatten pairs)))

(defn reverse-map
  "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
  [amap]
  (reduce (fn [m [k v]]
            (let [existing (get m v [])]
              (assoc m v (conj existing k))))
          {} amap))

(defn count= [& args]
  (apply = (map count args)))

(def not-count=
  (complement count=))

(defn- clean-nil-bindings [bindings]
  (let [pairs (partition 2 bindings)]
    (mapcat identity (filter #(first %) pairs))))

(defn meta-conj
  "Returns the supplied symbol with the supplied `attr` map conj-ed
  onto the symbol's current metadata."
  [sym attr]
  (with-meta sym (if (meta sym)
                   (conj (meta sym) attr)
                   attr)))

(defn set-namespace-value
  "Merges the supplied kv-pair into the metadata of the namespace in
  which the function is called."
  [key-name newval]
  (alter-meta! *ns* merge {key-name newval}))

(defn mk-destructured-seq-map [& bindings]
  ;; lhs needs to be symbolified
  (let [bindings (clean-nil-bindings bindings)
        to-sym (fn [s] (if (keyword? s) s (symbol s)))
        [lhs rhs] (unweave bindings)
        lhs  (for [l lhs] (if (sequential? l) (vec (map to-sym l)) (symbol l)))
        rhs  (for [r rhs] (if (sequential? r) (vec r) r))
        destructured (vec (destructure (interleave lhs rhs)))
        syms (first (unweave destructured))
        extract-code (vec (for [s syms] [(str s) s]))]
    (eval
     `(let ~destructured
        (into {} ~extract-code)))))

(def default-serializations
  ["org.apache.hadoop.io.serializer.WritableSerialization"
   "cascading.tuple.hadoop.BytesSerialization"
   "cascading.tuple.hadoop.TupleSerialization"])

(defn serialization-entry
  [serial-vec]
  (->> serial-vec
       (map (fn [x]
              (cond (string? x) x
                    (class? x) (.getName x))))
       (s/join ",")))

(defn no-empties [s]
  (when s (not= "" s)))

(defn merge-serialization-strings
  [& all]
  (serialization-entry
   (->> (filter no-empties all)
        (map #(s/split % #","))
        (apply merge-to-vec default-serializations))))

(defn stringify [x]
  (if (class? x)
    (.getName x)
    (str x)))

(defn resolve-collections [v]
  (->> (collectify v)
       (map stringify)
       (s/join ",")))

(defn adjust-vals [& vals]
  (->> (map resolve-collections vals)
       (apply merge-serialization-strings)))


(defn conf-merge [& ms]
  (->> ms
       (map #(update-vals % (fn [_ v] (resolve-collections v))))
       (reduce merge)))

(defn project-merge [& ms]
  (let [vals (->> (map #(get % "io.serializations") ms)
                  (apply adjust-vals))
        ms (apply conf-merge ms)]
    (assoc ms "io.serializations" vals)))

(defn stringify-keys [m]
  (into {} (for [[k v] m]
             [(if (keyword? k)
                (name k)
                (str k)) v])))

(defn try-parse-num [^String s]
  (try
    (Long/parseLong s)
    (catch NumberFormatException _
      nil )))

(defn recent-eval? [v]
  (let [m (meta v)
        ^String name (-> m :name str)]
    (and (= "clojure.core" (:ns m))
         (.startsWith name "*")
         (try-parse-num (.substring name 1))         
         )))

(defn search-for-var [val]
  ;; get all of them, filter out *1, *2, and *3, sort by static -> dynamic
  (->> (all-ns)
       (map ns-map)
       (mapcat identity)
       (map second)
       (filter #(and (var? %) (= (var-get %) val)))
       (filter (complement recent-eval?))
       (sort-by (fn [v] (if (-> v meta :dynamic) 1 0)))
       first ))
