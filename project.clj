(def shared
  '[[org.clojure/tools.macro "0.1.1"]
    [cascading/cascading-hadoop "2.0.0"
     :exclusions [org.codehaus.janino/janino
                  org.apache.hadoop/hadoop-core]]
    [org.clojure/tools.macro "0.1.1"]
    [cascading.kryo "0.3.1"]
    [cascalog/carbonite "1.2.1"]
    [log4j/log4j "1.2.16"]
    [hadoop-util "0.2.8"]
    [com.twitter/maple "0.2.0"]
    [jackknife "0.1.2"]])

(defproject cascalog/cascalog "1.9.0-SNAPSHOT"
  :description "Hadoop without the Hassle."
  :url "http://www.cascalog.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "Cascalog user mailing list"
                 :archive "https://groups.google.com/d/forum/cascalog-user"
                 :post "cascalog-user@googlegroups.com"}
  :min-lein-version "2.0.0"
  :jvm-opts ["-Xmx768m" "-server" "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :codox {:include [cascalog.vars cascalog.ops cascalog.io cascalog.api]}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :plugins [[lein-midje "2.0.0-SNAPSHOT"]
            [lein-swank "1.4.2"]
            [lein-clojars "0.8.0"]]
  :aliases {"all" ["with-profile" "dev:1.2,dev:1.3"]}
  :dependencies ~(conj shared '[org.clojure/clojure "1.4.0"])
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]
  :profiles {:all {:dependencies ~shared}
             :1.2 {:dependencies [[org.clojure/clojure "1.2.1"]]}
             :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                    [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]}})
