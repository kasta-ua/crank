(ns crank.kafka
  (:require [clojure.string :as str])
  (:import [org.apache.kafka.common.serialization ByteArrayDeserializer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer KafkaConsumer OffsetAndMetadata]
           [java.net InetAddress]))


(defn hostname []
  (-> (InetAddress/getLocalHost) .getHostName (str/split #"\.") first))


(defn normalize-config [config]
  (let [c (cond-> {:bootstrap.servers  (:uri config)
                   :group.id           (:group config)
                   :max.poll.records   (int (:batch-size config 10000))
                   :auto.offset.reset  "latest"
                   :enable.auto.commit false
                   :key.deserializer   ByteArrayDeserializer
                   :value.deserializer ByteArrayDeserializer}
            (:batch-bytes config) (assoc :fetch.max.bytes (int (:batch-bytes config))))]
    (merge c (dissoc config :uri :group :batch-size :batch-bytes))))


(defn make-consumer-config [config]
  (reduce-kv (fn [acc k v] (assoc acc (name k) v)) {} (normalize-config config)))


(defn make-consumer [config]
  (KafkaConsumer. (make-consumer-config config)))


;;; Utility helpers


(defn set-offset! [config topic offset]
  (let [consumer (make-consumer config)
        tp       (TopicPartition. topic 0)
        om       (OffsetAndMetadata. offset)]
    (try
      (.assign consumer [tp])
      (.commitSync consumer {tp om})
      (.position consumer tp)
      (finally
        (.close consumer)))))


(defn set-begin! [config topic]
  (let [consumer (make-consumer config)
        tp       (TopicPartition. topic 0)
        bof      (.beginningOffsets consumer [tp])
        om       (OffsetAndMetadata. (get bof tp))]
    (try
      (.assign consumer [tp])
      (.commitSync consumer {tp om})
      (.position consumer tp)
      (finally
        (.close consumer)))))


(defn get-offset [config topic]
  (let [consumer (make-consumer config)
        tp       (TopicPartition. topic 0)]
    (try
      (.assign consumer [tp])
      (.position consumer tp)
      (finally
        (.close consumer)))))
