(ns crank.kafka-fixtures
  (:require [clojure.test :as test :refer [deftest is]]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:import [java.time Duration]
           [org.apache.curator.test TestingServer]
           [kafka.server KafkaConfig KafkaServerStartable]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization StringSerializer]))


(def zk-address "127.0.0.1:2181")
(def kafka-address "127.0.0.1:9092")


(def *producer (atom nil))
(defn send! [topic message]
  (when (nil? @*producer)
    (reset! *producer (KafkaProducer. {"bootstrap.servers" kafka-address}
                        (StringSerializer.)
                        (StringSerializer.))))
  (.send @*producer (ProducerRecord. topic message)))


#_(deftest can-read-messages
  (let [input    (k/input {:servers kafka-address
                           :topic   "test"
                           :group   "test"})
        _        (log/info "acquiring input...")
        input    (input/acquire input)
        ;; .subscribe is lazy, this forces it
        _        (.poll (:consumer input) (Duration/ofMillis 1))
        _        (send-message "test" "qweqwe")
        _        (log/info "reading messages...")
        messages (input/receive input 100)]
    (println messages)
    (is (= 1 (count messages)))))


(defn cleanup-temp [f]
  (fs/delete-dir "/tmp/kafka-logs")
  (fs/delete-dir "/tmp/zk")
  (f))


(defn start-zookeeper [f]
  (log/info "ZK starting up")
  (let [zk (TestingServer. 2181 (io/file "/tmp/zk"))]
    (log/info "ZK started")
    (f)
    (.close zk)
    (log/info "ZK stopped")))


(defn -start-kafka []
  (let [config (KafkaConfig. {"zookeeper.connect"                zk-address
                              "listeners"                        "PLAINTEXT://127.0.0.1:9092"
                              "auto.create.topics.enable"        true
                              "offsets.topic.replication.factor" (short 1)
                              "offsets.topic.num.partitions"     (int 1)})]
    (doto (KafkaServerStartable. config)
      (.startup))))


(defn start-kafka [f]
  (log/info "kafka starting up")
  (let [kafka (-start-kafka)]
    (log/info "kafka started")
    (f)
    (.shutdown kafka)
    (log/info "kafka stopped")))

