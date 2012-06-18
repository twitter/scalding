(ns cascalog.conf-test
  (:use cascalog.api
        [midje sweet cascalog])
  (:require [clojure.string :as s]
            [cascalog.conf :as conf]
            [cascalog.util :as u])
  (:import [cascalog.hadoop DefaultComparator]
           [cascading.flow.planner PlannerException]))

(def comma
  (partial s/join ","))

(def defaults
  (comma u/default-serializations))

(fact "test JobConf bindings."
  (with-job-conf {"key" "val"}
    (fact "The first binding level should set the JobConf equal to the
      supplied conf map."
      conf/*JOB-CONF* => {"key" "val"}))
  
  (with-job-conf {"key" ["val1" "val2"]
                  "other-key" "other-val"}
    (fact "Vectors of strings will be joined w/ commas. (This binding
      level knocks out the previous, as the keys are identical."
      conf/*JOB-CONF* => {"key" "val1,val2"
                          "other-key" "other-val"})
    
    (with-job-conf {"key" ["val3"]}
      (fact "other-key from above should be preserved."
        conf/*JOB-CONF* => {"key" "val3"
                            "other-key" "other-val"})))
  
  (with-job-conf {"io.serializations" "java.lang.String"}
    conf/*JOB-CONF* => {"io.serializations" "java.lang.String"}
    (fact
      "Calling project-merge on the JobConf will prepend default
       serializations onto the supplied list of serializations."
      (u/project-merge conf/*JOB-CONF*) => {"io.serializations"
                                            (comma [defaults "java.lang.String"])})

    (with-serializations [String]
      (fact
        "You can specify serialiations using the `with-serializations`
      form. This works w/ class objects or strings. Without
      project-merge, the *JOB-CONF* variable is unaffected. (Note that
      classes are resolved properly.)"
        conf/*JOB-CONF* => {"io.serializations" "java.lang.String"})

      (fact "Again, project-merging with w/ Class objects, vs Strings."
        (u/project-merge conf/*JOB-CONF*) => {"io.serializations"
                                              (comma [defaults "java.lang.String"])})))
  
  (with-serializations [String]
    (with-job-conf {"io.serializations" "java.lang.String,SomeSerialization"}
      (fact "with-serializations nests properly with with-job-conf."
        (u/project-merge conf/*JOB-CONF*) => {"io.serializations"
                                              (comma [defaults
                                                      "java.lang.String"
                                                      "SomeSerialization"])}))))

(facts "Tests of various aspects of Kryo serialization."
  (with-job-conf
    {"cascading.kryo.serializations" "java.util.DoesntExist"
     "cascading.kryo.skip.missing" true
     "cascading.kryo.accept.all" true}
    (let [cal-tuple [[(java.util.GregorianCalendar.)]]]
      (fact?<- cal-tuple
               [?a]
               (cal-tuple ?a)) ))

  (with-job-conf
    {"cascading.kryo.accept.all" false}
    (let [cal-tuple [[(java.util.GregorianCalendar.)]]]
      (fact
        "Attempting to serialize an unregistered object when
      accept.all is set to false should throw a flow exception."
        (??<- [?a] (cal-tuple ?a))) => (throws PlannerException))))

(tabular
 (fact "Test of various comparators."
   (let [comp (DefaultComparator.)]
     (.compare comp ?left ?right) => ?expected))
  ?left     ?right       ?expected
  0         0            0
  1         1            0
  1         1M           0
  1M        1            0
  2M        1            1
  (Long. 4) (Integer. 3) 1
  (Long. 3) (Integer. 4) -1
  1         2M           -1)
