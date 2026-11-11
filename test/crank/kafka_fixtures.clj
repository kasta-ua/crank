(ns crank.kafka-fixtures
  (:require [clojure.tools.logging :as log])
  (:import [org.apache.kafka.common.test KafkaClusterTestKit KafkaClusterTestKit$Builder
            TestKitNodes$Builder]
           [org.apache.kafka.clients.admin Admin NewTopic]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.errors TopicExistsException]
           [org.apache.kafka.common.serialization StringSerializer]
           [java.util.concurrent ExecutionException]))


(def *cluster (atom nil))
(def *address (atom nil))
(def *producer (atom nil))


(defn kafka-address
  "Bootstrap address of the embedded broker. The test kit picks a free port, so
  unlike the old fixture there is no fixed 9092 to hardcode."
  []
  (or @*address
      (throw (ex-info "kafka fixture is not running" {}))))


(defn send! [topic message]
  (when (nil? @*producer)
    (reset! *producer (KafkaProducer. {"bootstrap.servers" (kafka-address)}
                        (StringSerializer.)
                        (StringSerializer.))))
  (.send @*producer (ProducerRecord. topic message)))


(defn create-topic!
  "Creates the topic up front, single partition.

  Not optional despite auto.create.topics.enable: a consumer that subscribes to
  a topic which does not exist yet gets its partition only after the producer
  creates it, and with auto.offset.reset=latest it then starts past the message
  the test has just sent. The old ZooKeeper fixture happened to win that race;
  making the topic first removes it instead of relying on luck."
  [topic]
  (with-open [admin (Admin/create {"bootstrap.servers" (kafka-address)})]
    (try
      (-> (.createTopics admin [(NewTopic. topic 1 (short 1))])
        .all
        (.get))
      (catch ExecutionException e
        (when-not (instance? TopicExistsException (.getCause e))
          (throw e))))))


(defn wait-for
  "Waits until pred returns something truthy, up to timeout-ms. Returns that
  value, or throws — beats sleeping a fixed amount and hoping."
  ([pred] (wait-for pred 15000))
  ([pred timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (or (pred)
         (if (< deadline (System/currentTimeMillis))
           (throw (ex-info (format "condition not met in %s ms" timeout-ms) {}))
           (do (Thread/sleep 50)
               (recur))))))))


(defn -start-kafka []
  (let [nodes   (-> (TestKitNodes$Builder.)
                  (.setNumBrokerNodes 1)
                  (.setNumControllerNodes 1)
                  (.setCombined true)
                  (.build))
        cluster (-> (KafkaClusterTestKit$Builder. nodes)
                  (.setConfigProp "auto.create.topics.enable" "true")
                  (.setConfigProp "offsets.topic.replication.factor" "1")
                  (.setConfigProp "offsets.topic.num.partitions" "1")
                  (.setConfigProp "transaction.state.log.replication.factor" "1")
                  (.setConfigProp "transaction.state.log.min.isr" "1")
                  (.build))]
    (.format cluster)
    (.startup cluster)
    (.waitForReadyBrokers cluster)
    cluster))


(defn start-kafka
  "Single-node KRaft broker for the duration of the suite.

  Kafka 4 removed ZooKeeper and kafka.server.KafkaServerStartable with it, so
  the previous TestingServer + KafkaServerStartable pair no longer exists.
  KafkaClusterTestKit is the supported replacement: it formats its own storage,
  manages its own temp directories (hence no cleanup-temp fixture) and shuts
  down on close."
  [f]
  (log/info "kafka starting up")
  (let [^KafkaClusterTestKit cluster (-start-kafka)]
    (reset! *cluster cluster)
    (reset! *address (.bootstrapServers cluster))
    (log/infof "kafka started at %s" (kafka-address))
    (try
      (f)
      (finally
        ;; the old fixture leaked the broker whenever a test threw
        (when-let [p @*producer]
          (.close p))
        (reset! *producer nil)
        (reset! *address nil)
        (reset! *cluster nil)
        (.close cluster)
        (log/info "kafka stopped")))))
