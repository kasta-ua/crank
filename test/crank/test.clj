(ns crank.test
  (:require [clojure.test :as test]
            [crank
             core-test]))


(defn -main [& args]
  (test/run-all-tests #"crank.*test"))
