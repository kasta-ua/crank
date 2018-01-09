(ns crank.kafka-test
  (:require [clojure.test :as test :refer [deftest is]]
            [crank.input :as input]
            [crank.kafka :as k]
            [clojure.tools.logging :as log])
  (:import [org.apache.curator.test TestingServer]
           [kafka.server KafkaConfig KafkaServerStartable]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization StringSerializer]))


(def zk-address "127.0.0.1:2181")
(def kafka-address "127.0.0.1:9092")


(deftest can-connect
  (let [input    (k/input {:servers            kafka-address
                           :topic              "test"
                           :group              "test"
                           "auto.offset.reset" "earliest"})
        _        (log/debug "acquiring input...")
        input    (input/acquire input)
        _        (log/debug (.listTopics (:consumer input)))
        _        (log/debug "reading messages...")
        messages (input/receive input 0)]
    (println messages)))


(defn start-zookeeper [f]
  (let [zk (TestingServer. 2181)]
    (log/debug "ZK started")
    (f)
    (.close zk)))


(defn start-kafka [f]
  (let [config (KafkaConfig. {"zookeeper.connect"         zk-address
                              "port"                      (int 9092)
                              "auto.create.topics.enable" true})
        kafka  (KafkaServerStartable. config)]
    (.startup kafka)
    (log/debug "kafka started")
    (f)
    (.shutdown kafka)))


(defn gen-messages [f]
  (let [producer (KafkaProducer. {"bootstrap.servers" kafka-address}
                   (StringSerializer.)
                   (StringSerializer.))]
    (.send producer (ProducerRecord. "test" "tralala"))
    (.send producer (ProducerRecord. "test" "qweqwe"))
    (.send producer (ProducerRecord. "test" "asdasd"))
    (.flush producer)
    (log/debug "messages sent")
    (f)
    (.close producer)))


(test/use-fixtures :once start-zookeeper start-kafka gen-messages)
