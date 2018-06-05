(ns crank.job
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]

            [crank.kafka :as kafka]))


(defn record->message [record]
  {:topic     (.topic record)
   :partition (.partition record)
   :offset    (.offset record)
   :timestamp (.timestamp record)
   :key       (.key record)
   :value     (json/parse-string (slurp (.value record)) true)})


(defn run-job [stop {:keys [kafka topic job-name func send-report]}]
  (let [consumer (kafka/make-consumer kafka)]
    (try
      (.subscribe consumer [topic])
      (loop [records nil]

        (send-report {:time     (System/currentTimeMillis)
                      :job-name job-name
                      :topic    topic
                      :type     :poll
                      ;; ConsumerRecords, `count` doesn't work
                      :count    (if records (.count records) 0)})

        (doseq [record records
                :let   [message (record->message record)]]
          (when @stop
            (throw (ex-info "stop iteration" {:stop true})))
          (func message)
          (send-report {:time      (System/currentTimeMillis)
                        :job-name  job-name
                        :topic     topic
                        :type      :message
                        :offset    (:offset message)
                        :partition (:partition message)}))

        (.commitSync consumer)

        (if @stop
          (throw (ex-info "stop job" {:stop true}))
          (recur (.poll consumer 100))))

      (catch Exception e
        (if (:stop (ex-data e))
          (log/infof "Stopping job %s" job-name)
          (throw e)))
      (finally
        (.close consumer)))))


(defn start-job [{:keys [job-name topic] :as config}]
  (log/infof "Starting job %s" job-name)

  (let [stop   (atom false)
        worker (doto (Thread. #(run-job stop config))
                 (.start))]
    {:config config
     :worker worker
     :report {:time     (System/currentTimeMillis)
              :job-name job-name
              :topic    topic
              :type     :start}
     :stop!  #(reset! stop true)}))
