(ns crank.test
  (:require [clojure.test :as test]
            [crank
             kafka-test
             worker-test]))


(defn -main [& args]
  (test/run-all-tests #"crank.*test"))
