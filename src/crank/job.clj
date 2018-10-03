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


(defn run-loop [consumer stop {:keys [job-name func send-report topics attempts]
                               :or   {attempts 0}
                               :as   config}]

  (let [job-id (.getId (Thread/currentThread))]
    (send-report {:time     (System/currentTimeMillis)
                  :job-name job-name
                  :job-id   job-id
                  :type     :start
                  :attempts attempts})
    (.subscribe consumer topics)

    (try
      (loop [messages nil]
        ;; messages are at least an empty vector, except for the first time
        (when-not (nil? messages)
          (send-report {:time     (System/currentTimeMillis)
                        :job-name job-name
                        :job-id   job-id
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
                          :job-id    job-id
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
                          :job-id   job-id
                          :type     :stop})
            (log/infof "stopping job %s" job-name))
          (do
            (log/errorf e "job %s died" job-name)
            (send-report {:time      (System/currentTimeMillis)
                          :job-name  job-name
                          :job-id    job-id
                          :type      :exception
                          :exception e})
            (throw e))))

      (finally
        (.close consumer)
        (log/debugf "job %s loop exiting" job-name)))))


(defn start-job [{:keys [kafka monitor-name job-name] :as config}]
  (log/infof "starting job %s" job-name)

  (let [consumer (kafka/make-consumer kafka)
        stop     (atom false)
        worker   (Thread. #(run-loop consumer stop config))]

    (.setName worker (str monitor-name "-" job-name "-" (.getId worker)))
    (.start worker)

    {:config config
     :worker worker
     :report []
     :stop!  #(do (reset! stop true)
                  (.wakeup consumer))}))
