(ns crank.core
  (:require [clojure.tools.logging :as log]
            [crank.job :as job]
            [clojure.string :as str]))


(defprotocol IMonitor
  (start [this job-name config])
  (stop [this] [this job-name]))


(defn update-vals [m f]
  (into {} (map (fn [[k v]] [k (f k v)]) m)))


(defn cons-limit [coll limit item]
  (->> (cons item coll)
       (take limit)
       doall))


(defn add-report [job report]
  (when job
    (update job :report cons-limit 10 report)))


(defn check-timeout [attempts issue-time timeout]
  (let [diff (- (System/currentTimeMillis) issue-time)
        to-check (min (* 10 60 1000) ; 10 mins is a maximum
                   (* timeout (Math/pow 2 attempts)))]
    (when (> diff to-check)
      diff)))


(defn check-job [job-name {:keys [stop! report config worker] :as job}]
  (if-let [issue (first report)]
    (let [{:keys [attempts time] :or {attempts 0}} issue

          diff (check-timeout attempts time (:timeout config 10000))]

      (if diff
        (do
          (log/infof "job %s [%s] seems to be dead since %s ms ago: %s\n%s"
            job-name
            (.getName worker)
            diff
            (pr-str report)
            (str/join "\n" (.getStackTrace worker)))
          (stop!)
          (job/start-job (assoc config :attempts (inc attempts))))
        job))
    job))


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
                       (.setDaemon true)
                       (.start))))

    (let [send-report (fn [r]
                        (log/debugf "new report for %s: %s" job-name r)
                        (swap! *running update job-name add-report r)
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
