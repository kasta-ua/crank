(ns crank.job
  (:require [clojure.tools.logging :as log]

            [crank.kafka :as kafka])
  (:import [org.apache.kafka.common.errors WakeupException]))


(defn record->message [record]
  {:topic     (.topic record)
   :partition (.partition record)
   :offset    (.offset record)
   :timestamp (.timestamp record)
   :key       (.key record)
   :value     (.value record)})


(defn process-batch [batch stop {:keys [job-name func send-report]}]
  (when @stop
    (throw (ex-info "stop batch" {:stop true})))

  (func batch)

  (send-report {:time         (System/currentTimeMillis)
                :job-name     job-name
                :type         :batch
                :topic        (-> batch first :topic)
                :start-offset (-> batch first :offset)
                :end-offset   (-> batch last  :offset)
                :partition    (-> batch first :partition)}))


(defn run-loop [consumer stop {:keys [job-name func send-report] :as config}]
  (send-report {:time     (System/currentTimeMillis)
                :job-name job-name
                :type     :start})

  (try
    (loop [messages nil]
      (when-not (nil? messages) ; which is always except for first time
        (send-report {:time     (System/currentTimeMillis)
                      :job-name job-name
                      :type     :poll
                      :count    (count messages)}))

      (if (:batch? config)
        (when (seq messages)
          (process-batch messages stop config))
        (doseq [message messages]
          (when @stop
            (throw (ex-info "stop iteration" {:stop true})))

          (func message)

          (send-report {:time      (System/currentTimeMillis)
                        :job-name  job-name
                        :type      :message
                        :topic     (:topic message)
                        :offset    (:offset message)
                        :partition (:partition message)})))

      (when (seq messages)
        (.commitSync consumer))

      (if @stop
        (throw (ex-info "stop job" {:stop true}))
        (do
          (recur (->> (.poll consumer 100)
                      (mapv record->message))))))

    (catch Exception e
      (if (or (:stop (ex-data e))
              (and @stop
                   (instance? WakeupException e)))
        (do
          (send-report {:time     (System/currentTimeMillis)
                        :job-name job-name
                        :type     :stop})
          (log/infof "stopping job %s" job-name))
        (do
          (log/errorf e "job %s died" job-name)
          (send-report {:time      (System/currentTimeMillis)
                        :job-name  job-name
                        :type      :exception
                        :exception e})
          (throw e))))

    (finally
      (.close consumer)
      (log/debugf "job %s loop exiting" job-name))))


(defn start-job [{:keys [kafka monitor-name job-name topics] :as config}]
  (log/infof "starting job %s" job-name)

  (let [consumer (kafka/make-consumer kafka)]
    (.subscribe consumer topics)

    (let [stop   (atom false)
          worker (Thread. #(run-loop consumer stop config))]

      (.setName worker (str monitor-name "-" job-name "-" (.getId worker)))
      (.start worker)

      {:config   config
       :worker   worker
       :report   []
       :stop!    #(do (reset! stop true)
                      (.wakeup consumer))})))
