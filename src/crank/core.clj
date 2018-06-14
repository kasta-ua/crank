(ns crank.core
  (:require [clojure.tools.logging :as log]
            [crank.job :as job]))


(defprotocol IMonitor
  (start [this job-name config])
  (stop [this] [this job-name]))


(defn update-vals [m f]
  (into {} (for [[k v] m]
             [k (f k v)])))


(defn cons-limit [coll limit item]
  (->> (cons item coll)
       (take limit)
       doall))


(defn check-job [job-name {:keys [stop! report config] :as job}]
  (when (first report)
    (let [diff (- (System/currentTimeMillis)
                  (:time (first report)))]
      (if (> diff (:timeout config 10000))
        (do
          (log/infof "job %s seems to be dead since %s ms ago: %s"
            job-name diff (pr-str report))
          (stop!)
          (job/start-job config))
        job))))


(defn run-master [running]
  (try
    (loop []
      (swap! running update-vals check-job)
      (Thread/sleep 1000)
      (recur))

    (catch InterruptedException e
      (log/debug "monitor master exiting"))))


(defrecord Monitor [uname reporting-cb *running master]
  IMonitor
  (start [this job-name config]
    (when-let [old-job (get @*running job-name)]
      (throw (ex-info (str "job is running already: " job-name)
               {:job old-job})))

    (when (or (nil? @master)
              (not (.isAlive @master)))
      (reset! master (doto (Thread. #(run-master *running))
                       (.setName (str uname "-master"))
                       (.start))))

    (let [send-report (fn [r]
                        (log/debugf "new report for %s: %s" job-name r)
                        (swap! *running update-in [job-name :report] cons-limit 10 r)
                        (when reporting-cb
                          (reporting-cb r)))
          job         (job/start-job (assoc config
                                       :monitor-name uname
                                       :job-name job-name
                                       :send-report send-report))]
      (swap! *running assoc job-name job)))

  (stop [this]
    (doseq [job-name (keys @*running)]
      (stop this job-name))
    (when (and @master (.isAlive @master))
      (.interrupt @master)))

  (stop [this job-name]
    (swap! *running
      (fn [jobs]
        (let [{:keys [stop!]} (get jobs job-name)]
          (when stop!
            (log/infof "asking job %s to stop" job-name)
            (stop!)))
        (dissoc jobs job-name)))))


(defn init
  ([] (init {}))
  ([{:keys [name report]
     :or   {name "crank"}}]
   (map->Monitor {:uname        name
                  :reporting-cb report
                  :*running     (atom {})
                  :master       (atom nil)})))
