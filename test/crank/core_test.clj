(ns crank.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [crank.core :as crank]

            [crank.kafka-fixtures :as ck]))


(def counter (atom 0))
(defn get-name []
  (str "crank" (swap! counter inc)))


(defn start [topic func]
  (let [last-report (atom nil)
        start-time  (System/currentTimeMillis)
        mon         (crank/init {:name   (get-name)
                                 :report (fn [x]
                                           (reset! last-report x))})]
    (crank/start mon topic {:topics [topic]
                            :func   func
                            :kafka  {:uri   "127.0.0.1:9092"
                                     :group "q"}})
    [mon
     (future
       (loop []
         (cond
           (< 10000 (- (System/currentTimeMillis) start-time))
           (throw (ex-info "waited for 10 seconds, I have no patience!" {}))

           (= (:type @last-report) :poll)
           :started

           :else
           (do (Thread/sleep 100)
               (recur)))))]))


(deftest simple-job
  (testing "just start and stop"
    (let [mon (crank/init {:name (get-name)})]
      (crank/start mon "simple" {:topics ["simple"]
                                 :func   identity
                                 :kafka  {:uri   "127.0.0.1:9092"
                                          :group "q"}})
      (crank/stop mon)))

  (testing "starting a job and processing a message works"
    (let [topic             "simple2"
          messages          (atom [])
          [mon has-started] (start topic
                              (fn [message]
                                (swap! messages conj message)))]
      (when @has-started
        :go)

      @(ck/send! topic "just a value")
      (Thread/sleep 100)
      (crank/stop mon)

      (is (= 1 (count @messages)))
      (is (= "just a value" (-> @messages first :value slurp))))))


(defn report-threads [f]
  (f)
  (println "Still running threads:")
  (doseq [t (sort (map #(.getName %) (.keySet (Thread/getAllStackTraces))))]
    (prn t)))


(test/use-fixtures :once
  report-threads
  (fn [f] (f) (shutdown-agents))
  ck/cleanup-temp ck/start-zookeeper ck/start-kafka)
