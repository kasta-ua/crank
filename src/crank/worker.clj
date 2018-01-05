(ns crank.worker
  (:require [clojure.tools.logging :as log]

            [crank.input :as input]))


(defprotocol IProcessor
  (start! [this]))

(defprotocol IStoppable
  (stop [this]))


(defrecord Processor [input data
                      ;; own
                      thread stop]
  IProcessor
  (start! [this]
    (let [stop     (atom false)
          new-this (assoc this :stop stop)
          thread   (Thread. new-this)]
      (.start thread)
      (assoc new-this :thread thread)))

  IStoppable
  (stop [this]
    (reset! stop true))

  Runnable
  (run [this]
    (let [func (:func data)]
      (try
        (loop [messages (input/receive input 100)]
          (doseq [message messages]
            (log/debug "processing message" message)
            (func message)
            (input/ack input message))
          (when-not @stop
            (recur (input/receive input 100))))
        (catch Exception e
          (log/error e)
          ;; FIXME: properly stop job and exit here
          :exit)))))


;; TODO: check jobs on valid format
(defn start [count jobs]
  (loop [count   count
         jobs    jobs
         started {}]

    (let [job (first jobs)]
      (if-not (and job (pos? count))

        started

        (let [[job-name {:keys [input] :as data}] job]

          (if-let [input (input/acquire input)]
            (let [processor (map->Processor {:input input
                                             :data  data})]
              (recur
                (dec count)
                (rest jobs)
                (assoc started job-name (start! processor))))

            (recur
              count
              (rest jobs)
              started)))))))
