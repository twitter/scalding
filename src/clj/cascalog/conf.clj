(ns cascalog.conf
  (:use [cascalog.util :only (project-merge)]
        [clojure.java.io :only (resource)])
  (:require [jackknife.core :as u])
  (:import [cascading.flow FlowProps]))

(defn read-settings [x]
  (try (binding [*ns* (create-ns (gensym "settings"))]
         (refer 'clojure.core)
         (eval (read-string (str "(do " x ")"))))
       (catch RuntimeException e
         (u/throw-runtime "Error reading job-conf.clj!\n\n" e))))

(defn project-settings []
  (if-let [conf-path (resource "job-conf.clj")]
    (let [conf (-> conf-path slurp read-settings project-merge)]
      (u/safe-assert (map? conf)
                     "job-conf.clj must end with a map of config parameters.")
      conf)
    {}))

(def ^:dynamic *JOB-CONF* {})

(defn project-conf []
  (project-merge {FlowProps/DEFAULT_ELEMENT_COMPARATOR "cascalog.hadoop.DefaultComparator"}
                 (project-settings)
                 *JOB-CONF*
                 {"io.serializations"
                  "cascalog.hadoop.ClojureKryoSerialization"}))

(defn set-job-conf! [amap]
  (alter-var-root #'*JOB-CONF* (fn [& ignored] (into {} amap))))
