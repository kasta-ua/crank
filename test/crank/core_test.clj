(ns crank.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]

            [crank.core :as crank]
            [crank.kafka :as kafka]
            [crank.kafka-fixtures :as ck]))


(def counter (atom 0))
(defn get-name []
  (str "c" (swap! counter inc)))


(defn start [topic config]
  (let [last-report (atom nil)
        mon         (crank/init {:name   (get-name)
                                 :report (fn [x]
                                           (reset! last-report x))})]
    (ck/create-topic! topic)
    (crank/start mon topic (merge
                             {:topics [topic]
                              ;; earliest, not the "latest" default: otherwise
                              ;; whether the test sees its own message depends
                              ;; on the consumer joining the group before the
                              ;; producer sends. Own group per topic so the
                              ;; tests cannot resume each other's offsets.
                              :kafka  {:uri                (ck/kafka-address)
                                       :group              (str "q-" topic)
                                       :auto.offset.reset  "earliest"}}
                             config))
    [mon
     (future (ck/wait-for #(= (:type @last-report) :poll) 10000))]))


(deftest simple-job
  (testing "just start and stop"
    (let [mon (crank/init {:name (get-name)})]
      (crank/start mon "simple" {:topics ["simple"]
                                 :func   identity
                                 :kafka  {:uri   (ck/kafka-address)
                                          :group "q-simple"}})
      (crank/stop mon)))

  (testing "starting a job and processing a message works"
    (let [topic             "simple2"
          messages          (atom [])
          [mon has-started] (start topic
                              {:func (fn [message]
                                       (swap! messages conj message))})]
      @has-started

      @(ck/send! topic "just a value")
      (ck/wait-for #(= 1 (count @messages)))
      (crank/stop mon "simple2")

      (is (= 1 (count @messages)))
      (is (= "just a value" (some-> @messages first :value slurp)))))

  (testing "starting batch job and processing a batch works"
    (let [topic             "batch"
          batches           (atom [])
          [mon has-started] (start topic
                              {:func       (fn [batch]
                                             (swap! batches conj batch))
                               :batch?     true
                               :batch-size 2})]
      @has-started

      @(ck/send! topic "1")
      @(ck/send! topic "2")
      @(ck/send! topic "3")
      @(ck/send! topic "4")
      (ck/wait-for #(= 4 (count (apply concat @batches))))
      (crank/stop mon topic)

      (is (every? vector? @batches))
      (is (= ["1" "2" "3" "4"] (map (comp slurp :value) (apply concat @batches))))))

  (testing "what if an exception happened"
    (let [topic "exc"
          _     (ck/create-topic! topic)
          ;; a message to skip past, so the offset below is predictable
          _     @(ck/send! topic "create topic!")
          _     (kafka/set-offset! {:uri (ck/kafka-address) :group (str "q-" topic)} topic 1)

          oops-i-did-that   (atom false)
          messages          (atom [])
          [mon has-started] (start topic
                              {:timeout 5000
                               :func
                               (fn [message]
                                 (if-not @oops-i-did-that
                                   (do
                                     (reset! oops-i-did-that true)
                                     (throw (ex-info "Oops, I did that again" {})))
                                   (swap! messages conj message)))})]
      @has-started
      @(ck/send! topic "I played with your heart")
      ;; the first delivery throws, the job dies, the monitor restarts it after
      ;; :timeout and the redelivery goes through
      (ck/wait-for #(= 1 (count @messages)) 30000)
      (crank/stop mon)

      (is (= 1 (count @messages)))
      (is (= "I played with your heart" (some-> @messages first :value slurp))))))


(defn report-threads [f]
  (f)
  (println "Still running threads:")
  (doseq [t (sort (map #(.getName %) (.keySet (Thread/getAllStackTraces))))]
    (prn t)))


(test/use-fixtures :once
  report-threads
  (fn [f] (f) (shutdown-agents))
  ck/start-kafka)
