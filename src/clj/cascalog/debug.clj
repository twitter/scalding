(ns cascalog.debug)

(def ^:dynamic *DEBUG* false)

(defn debug-print [& args]
  (when *DEBUG* (apply println args)))
