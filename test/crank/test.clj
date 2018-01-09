(ns crank.test
  (:require [clojure.test :as test]
            [crank.kafka-test]))


(defn -main [& args]
  (test/run-all-tests #"crank.*test"))
