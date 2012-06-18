(ns cascalog.playground
  (:require [cascalog.conf :as conf])
  (:import [java.io PrintStream]
           [cascalog WriterOutputStream]
           [org.apache.log4j Logger WriterAppender SimpleLayout]))

(defn bootstrap []
  (use 'cascalog.api
       '[jackknife.seq :only (find-first)])
  (conf/set-job-conf! {"io.sort.mb" 1})
  (require '(cascalog [workflow :as w]
                      [ops :as c]
                      [vars :as v])))

(defn bootstrap-emacs []
  (bootstrap)
  (-> (Logger/getRootLogger)
      (.addAppender (WriterAppender. (SimpleLayout.) *out*)))
  (System/setOut (PrintStream. (WriterOutputStream. *out*))))

(defn bootstrap-jcascalog []
  (import '[com.twitter.maple.tap StdoutTap]
          '[jcascalog Api Fields Predicate PredicateMacro Subquery
            Api$FirstNArgs Option]
          '[jcascalog.op Sum Count Div Plus Minus Multiply Avg
            Max Min Limit LimitRank ReParse DistinctCount]))

(def person
  [
   ;; [person]
   ["alice"]
   ["bob"]
   ["chris"]
   ["david"]
   ["emily"]
   ["george"]
   ["gary"]
   ["harold"]
   ["kumar"]
   ["luanne"]
   ])

(def age
  [
   ;; [person age]
   ["alice" 28]
   ["bob" 33]
   ["chris" 40]
   ["david" 25]
   ["emily" 25]
   ["george" 31]
   ["gary" 28]
   ["kumar" 27]
   ["luanne" 36]
   ])

(def gender
  [
   ;; [person gender]
   ["alice" "f"]
   ["bob" "m"]
   ["chris" "m"]
   ["david" "m"]
   ["emily" "f"]
   ["george" "m"]
   ["gary" "m"]
   ["harold" "m"]
   ["luanne" "f"]
   ])

(def gender-fuzzy
  [
   ;; [person gender timestamp]
   ["alice" "f" 100]
   ["alice" "m" 102]
   ["alice" "f" 110]
   ["bob" "m" 100]
   ["bob" "m" 101]
   ["bob" "m" 102]
   ["bob" "f" 103]
   ["chris" "f" 100]
   ["chris" "m" 200]
   ["emily" "f" 100]
   ["george" "m" 100]
   ["george" "m" 101]
   ])


(def full-names
  [
   ;; [person full-name]
   ["alice" "Alice Smith"]
   ["bob" "Bobby John Johnson"]
   ["chris" "CHRIS"]
   ["david" "A B C D E"]
   ["emily" "Emily Buchanan"]
   ["george" "George Jett"]
   ])

(def location
  [
   ;; [person country state city]
   ["alice" "usa" "california" nil]
   ["bob" "canada" nil nil]
   ["chris" "usa" "pennsylvania" "philadelphia"]
   ["david" "usa" "california" "san francisco"]
   ["emily" "france" nil nil]
   ["gary" "france" nil "paris"]
   ["luanne" "italy" nil nil]
   ])

(def follows
  [
   ;; [person-follower person-followed]
   ["alice" "david"]
   ["alice" "bob"]
   ["alice" "emily"]
   ["bob" "david"]
   ["bob" "george"]
   ["bob" "luanne"]
   ["david" "alice"]
   ["david" "luanne"]
   ["emily" "alice"]
   ["emily" "bob"]
   ["emily" "george"]
   ["emily" "gary"]
   ["george" "gary"]
   ["harold" "bob"]
   ["luanne" "harold"]
   ["luanne" "gary"]
   ])

(def num-pair
  [
   [1 2]
   [0 0]
   [1 1]
   [4 4]
   [5 10]
   [2 7]
   [3 6]
   [8 64]
   [8 3]
   [4 0]
   ])

(def integer
  [
   [-1]
   [0]
   [1]
   [2]
   [3]
   [4]
   [5]
   [6]
   [7]
   [8]
   [9]
   ])

(def sentence
  [
   ["Four score and seven years ago our fathers brought forth on this continent a new nation"]
   ["conceived in Liberty and dedicated to the proposition that all men are created equal"]
   ["Now we are engaged in a great civil war testing whether that nation or any nation so"]
   ["conceived and so dedicated can long endure We are met on a great battlefield of that war"]
   ["We have come to dedicate a portion of that field as a final resting place for those who"]
   ["here gave their lives that that nation might live It is altogether fitting and proper"]
   ["that we should do this"]
   ["But in a larger sense we can not dedicate  we can not consecrate  we can not hallow"]
   ["this ground The brave men living and dead who struggled here have consecrated it"]
   ["far above our poor power to add or detract The world will little note nor long remember"]
   ["what we say here but it can never forget what they did here It is for us the living rather"]
   ["to be dedicated here to the unfinished work which they who fought here have thus far so nobly"]
   ["advanced It is rather for us to be here dedicated to the great task remaining before us "]
   ["that from these honored dead we take increased devotion to that cause for which they gave"]
   ["the last full measure of devotion  that we here highly resolve that these dead shall"]
   ["not have died in vain  that this nation under God shall have a new birth of freedom"]
   ["and that government of the people by the people for the people shall not perish"]
   ["from the earth"]
   ])

(def duprows
  [
   [1 2 3]
   [1 2 3]
   [1 2 5]
   [1 3 6]
   [2 5 7]
   [2 2 2]
   ])

(def dirty-ages
  [
   ;; [timestamp name age]
   [1200 "alice" 20]
   [1000 "bob" 25]
   [1500 "harry" 46]
   [1800 "alice" 19]
   [2000 "bob" 30]
   ])

(def dirty-follower-counts
  [
   ;; [timestamp name follower-count]
   [2000 "gary" 56]
   [1100 "george" 124]
   [1900 "gary" 49]
   [3000 "juliette" 1002]
   [3002 "juliette" 1010]
   [3001 "juliette" 1011]
   ])
