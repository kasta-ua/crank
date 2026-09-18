(ns crank.core
  (:require [clojure.tools.logging :as log]
            [crank.job :as job]
            [clojure.string :as str]))


(defprotocol IMonitor
  (start [this job-name config])
  (stop [this] [this job-name]))


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


(defn job-timeout
  "How long the job has been silent, if it is over its timeout; nil when alive.
  Pure, so it is safe to call from anywhere."
  [{:keys [report config]}]
  (when-let [{:keys [attempts time] :or {attempts 0}} (first report)]
    (check-timeout attempts time (:timeout config 10000))))


(defn- restart-job!
  "Stops a dead job and starts a fresh one. Side-effecting: spawns a consumer
  thread, so it must never run inside swap!."
  [job-name {:keys [stop! report config worker]} diff]
  (log/infof "job %s [%s] seems to be dead since %s ms ago: %s\n%s"
    job-name
    (.getName worker)
    diff
    (pr-str report)
    (str/join "\n" (.getStackTrace worker)))
  (stop!)
  (job/start-job (assoc config :attempts (inc (:attempts (first report) 0)))))


(defn- install-job!
  "Puts the restarted job in place of the old one. The function handed to
  swap-vals! is pure, so a lost CAS costs nothing but a retry. Returns false
  when the entry was stopped or replaced meanwhile — the worker we have just
  started is then unwanted."
  [running job-name old new]
  (let [[before _] (swap-vals! running
                     (fn [jobs]
                       (if (identical? (get jobs job-name) old)
                         (assoc jobs job-name new)
                         jobs)))]
    (identical? (get before job-name) old)))


(defn check-jobs!
  "One monitor pass: restarts the jobs that stopped reporting.

  Restarting happens outside of swap! on purpose. swap! reruns its function on
  a lost CAS and does not undo side effects, while *running is swapped by
  send-report on every single message of every job — so contention is constant,
  not exceptional. Deciding and restarting in one swap! (as check-job used to)
  left orphaned consumers behind on every retry: alive, in the group, but
  missing from *running and therefore invisible to the monitor and unreachable
  by stop!."
  [running]
  (doseq [[job-name job] @running
          :let  [diff (job-timeout job)]
          :when diff]
    (let [new-job (restart-job! job-name job diff)]
      (when-not (install-job! running job-name job new-job)
        (log/infof "job %s is gone, stopping the worker we just started" job-name)
        ((:stop! new-job))))))


(defn run-master [running]
  (try
    (loop []
      (check-jobs! running)
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

  ;; dissoc first, stop! after: same reason as check-jobs! — a function handed
  ;; to swap! must be pure. Exactly one caller sees the entry in `before`, so a
  ;; racing stop cannot signal the same worker twice.
  (stop [this job-name]
    (let [[before _] (swap-vals! *running dissoc job-name)]
      (when-let [{:keys [stop!]} (get before job-name)]
        (log/infof "asking job %s to stop" job-name)
        (stop!)))))


(defn init
  ([] (init {}))
  ([{:keys [name report]
     :or   {name "crank"}}]
   (map->Monitor {:uname        name
                  :reporting-cb report
                  :*running     (atom {})
                  :master       (atom nil)})))
