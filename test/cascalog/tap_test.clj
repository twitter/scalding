(ns cascalog.tap-test
  (:use cascalog.tap
        [midje sweet cascalog]
        cascalog.testing)
  (:require [cascalog.io :as io]
            [cascalog.api :as api]
            [cascalog.workflow :as w])
  (:import [cascading.tuple Fields]
           [cascading.tap Tap]
           [cascading.tap.hadoop Hfs Lfs GlobHfs TemplateTap]))

(defn tap-source [tap]
  (if (map? tap)
    (recur (:source tap))
    tap))

(defn tap-sink [tap]
  (if (map? tap)
    (recur (:sink tap))
    tap))

(defn get-scheme [^Tap tap]
  (.getScheme tap))

(defn test-tap-builder [tap-func extracter]
  (fn [& opts]
    (extracter
     (apply tap-func
            (w/text-line ["line"] Fields/ALL)
            "/path/"
            opts))))

(def hfs-test-source (test-tap-builder hfs-tap tap-source))
(def hfs-test-sink (test-tap-builder hfs-tap tap-sink))
(def lfs-test-source (test-tap-builder lfs-tap tap-source))
(def lfs-test-sink (test-tap-builder lfs-tap tap-sink))

(tabular
 (fact "api outfields testing."
   (-> (apply api/hfs-textline "path" ?opts)
       (tap-sink)
       (.getSinkFields)) => ?fields)
 ?fields                ?opts
 Fields/ALL             []
 (w/fields ["?a"])      [:outfields ["?a"]]
 (w/fields ["?a" "!b"]) [:outfields ["?a" "!b"]])

(tabular
 (fact "Type testing on the various taps."
   ?tap => (fn [x] (instance? ?type x)))
 ?type        ?tap
 TemplateTap (hfs-test-sink :sink-template "%s/")
 GlobHfs     (hfs-test-source :source-pattern "%s/")
 Hfs         (hfs-test-sink :source-pattern "%s/")
 Hfs         (hfs-test-source)
 Lfs         (lfs-test-source))

(fact "SinkMode testing."
  (hfs-test-sink) => (memfn isKeep)
  (hfs-test-sink :sinkmode :keep) => (memfn isKeep)
  (hfs-test-sink :sinkmode :update) => (memfn isUpdate)
  (hfs-test-sink :sinkmode :replace) => (memfn isReplace))

(tabular
 (fact ":sinkparts keyword should tune sink settings."
   (-> (apply ?func ?opts)
       (get-scheme)
       (.getNumSinkParts)) => ?result)
 ?result ?func         ?opts
 3       hfs-test-sink [:sinkparts 3]
 3       lfs-test-sink [:sinkparts 3]
 0       hfs-test-sink []
 0       lfs-test-sink [])

(fact
  ":sink-template option should set path template on cascading tap."
  (.getPathTemplate (hfs-test-sink :sink-template "%s/"))) => "%s/"

(fact
  "Test executing tuples into a template tap and sourcing them back
  out with a source pattern."
  (io/with-log-level :fatal
    (io/with-fs-tmp [_ tmp]
      (let [tuples [[1 2] [2 3] [4 5]]
            temp-tap (api/hfs-seqfile (str tmp "/")
                                      :sink-template "%s/"
                                      :source-pattern "{1,2}/*")]
        temp-tap
        (api/?<- temp-tap [?a ?b] (tuples ?a ?b))
        temp-tap => (produces [[1 2] [2 3]])))))
