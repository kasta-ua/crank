(ns crank.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [crank.core :as crank]

            [crank.kafka-fixtures :as ck]))


(deftest simple-job
  (testing "just start and stop"
      (let [mon (crank/init)]
        (crank/start mon "simple" {:topics ["simple"]
                                   :func   identity
                                   :kafka  {:uri   "127.0.0.1:9092"
                                            :group "q"}})
      (crank/stop mon)))
  (testing "starting a job and processing a message works"
    (let [messages    (atom [])
          last-report (atom nil)
          start-time  (System/currentTimeMillis)
          mon         (crank/init {:report (fn [x]
                                             (reset! last-report x))})]
      (crank/start mon "simple" {:topics ["simple"]
                                 :func   (fn [message]
                                           (swap! messages conj message))
                                 :kafka  {:uri   "127.0.0.1:9092"
                                          :group "q"}})
      ;; consumer takes some time to connect to Kafka, wait
      (loop []
        (when (and (not= (:type @last-report) :poll)
                   (> 10000 (- (System/currentTimeMillis) start-time)))
          (Thread/sleep 100)
          (recur)))

      @(ck/send! "simple" "just a value")
      (Thread/sleep 100)
      (crank/stop mon)

      (is (= 1 (count @messages)))
      (is (= "just a value" (-> @messages first :value slurp))))))


(test/use-fixtures :once ck/cleanup-temp ck/start-zookeeper ck/start-kafka)
