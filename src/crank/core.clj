(ns crank.core
  (:require [clojure.tools.logging :as log]

            [crank.job :as job]))


(defprotocol IMonitor
  (start [this job-name config])
  (stop [this] [this job-name]))


(defn run-master [running]
  (try
    (loop []

      (doseq [[job-name {:keys [report config]}] @running

              :let  [diff (- (System/currentTimeMillis)
                             (:time report ))]
              :when (> diff (:timeout config 5000))]
        (do
          (log/infof "job %s seems to be dead" job-name)
          (swap! running assoc job-name (job/start-job config))))

      (Thread/sleep 1000)
      (recur))
    (catch InterruptedException e
      (log/debug "monitor master exiting")
      :pass)))


(defrecord Monitor [reporting-cb *running master]
  IMonitor
  (start [this job-name config]
    (when-let [old-job (get @*running job-name)]
      (throw (ex-info (str "job is running already: " job-name)
               {:job old-job})))

    (when (or (nil? @master)
              (not (.isAlive @master)))
      (reset! master (doto (Thread. #(run-master *running))
                       (.start))))

    (let [send-report (fn [r]
                        (swap! *running assoc-in [job-name :report] r)
                        (when reporting-cb
                          (reporting-cb r)))
          job         (job/start-job (assoc config
                                       :job-name job-name
                                       :send-report send-report))]
      (swap! *running assoc job-name job)))

  (stop [this]
    (doseq [job-name (keys @*running)]
      (stop this job-name))
    (when (and @master (.isAlive @master))
      (.interrupt @master)))

  (stop [this job-name]
    (let [{:keys [stop!]} (get @*running job-name)]
      (when stop!
        (stop!))
      (swap! *running dissoc job-name))))


(defn init
  ([] (init {}))
  ([{:keys [report]}]
   (map->Monitor
     {:reporting-cb report
      :*running     (atom {})
      :master       (atom nil)})))
