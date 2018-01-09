(ns crank.kafka-test
  (:require [clojure.test :as test :refer [deftest is]]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [crank.input :as input]
            [crank.kafka :as k])
  (:import [org.apache.curator.test TestingServer]
           [kafka.server KafkaConfig KafkaServerStartable]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization StringSerializer]))


(def zk-address "127.0.0.1:2181")
(def kafka-address "127.0.0.1:9092")


(def *producer (atom nil))
(defn send-message [topic message]
  (when (nil? @*producer)
    (reset! *producer (KafkaProducer. {"bootstrap.servers" kafka-address}
                        (StringSerializer.)
                        (StringSerializer.))))
  (.send @*producer (ProducerRecord. topic message)))


(deftest can-connect
  (let [input    (k/input {:servers kafka-address
                           :topic   "test"
                           :group   "test"})
        _        (log/info "acquiring input...")
        input    (input/acquire input)
        ;; .subscribe is lazy, this forces it
        _        (.poll (:consumer input) 1)
        _        (send-message "test" "qweqwe")
        _        (log/info "reading messages...")
        messages (input/receive input 100)]
    (println messages)
    (is (= 1 (count messages)))))


(defn clean-temp [f]
  (fs/delete-dir "/tmp/kafka-logs")
  (fs/delete-dir "/tmp/zk")
  (f))


(defn start-zookeeper [f]
  (let [zk (TestingServer. 2181 (io/file "/tmp/zk"))]
    (log/info "ZK started")
    (f)
    (.close zk)))


(defn start-kafka [f]
  (let [config (KafkaConfig. {"zookeeper.connect"                zk-address
                              "listeners"                        "PLAINTEXT://127.0.0.1:9092"
                              "auto.create.topics.enable"        true
                              "offsets.topic.replication.factor" (short 1)
                              "offsets.topic.num.partitions"     (int 1)})
        kafka  (KafkaServerStartable. config)]
    (.startup kafka)
    (log/info "kafka started")
    (f)
    (.shutdown kafka)))


(test/use-fixtures :once clean-temp start-zookeeper start-kafka)
