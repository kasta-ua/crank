(ns crank.kafka
  (:require [clojure.tools.logging :as log]

            [crank.input :as input])
  (:import org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.common.serialization.StringDeserializer))


(defn cr->data
  "Yield a clojure representation of a consumer record"
  [cr]
  {:key       (.key cr)
   :offset    (.offset cr)
   :partition (.partition cr)
   :topic     (.topic cr)
   :value     (.value cr)})


(defrecord KafkaInput [servers topic group kafka-opts
                       ;; inner
                       consumer]
  input/Input
  (acquire [this]
    ;; FIXME: сначала lock этого топика в ZK
    (when true
      (let [c (KafkaConsumer.
                (assoc kafka-opts
                  "bootstrap.servers" servers
                  "group.id"          group)
                (StringDeserializer.)
                (StringDeserializer.))]
        (.subscribe c [topic])
        (assoc this :consumer c))))

  (receive [this timeout]
    (let [crs (.poll (:consumer this) timeout)]
      (log/debugf "received %s records" (.count crs))
      (map cr->data crs)))

  (ack [this message]
    (.commitSync (:consumer this))))


(defn input [{:keys [servers topic group] :as opts}]
  (map->KafkaInput {:servers servers :topic topic :group group
                    :opts (dissoc opts :servers :topic :group)}))
