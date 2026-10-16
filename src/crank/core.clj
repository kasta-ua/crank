(ns crank.core
  (:require [clojure.tools.logging :as log]
            [crank.job :as job]
            [clojure.string :as str])
  (:import [org.apache.kafka.common KafkaException]))


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
  started is then unwanted.

  Compares workers, not whole entries: the entry keeps changing while the
  restart is under way — the fresh worker reports :start into it at once, a
  slow old one may still report — and only a different worker (or none at all)
  means the job was stopped or replaced."
  [running job-name old new]
  (let [same?      #(identical? (:worker (get % job-name)) (:worker old))
        [before _] (swap-vals! running
                     (fn [jobs]
                       (if (same? jobs)
                         (assoc jobs job-name new)
                         jobs)))]
    (same? before)))


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
    ;; A consumer that cannot be built (e.g. bootstrap servers not resolving)
    ;; must not take the master thread down with it: every job of the monitor
    ;; would stop being restarted. The old entry stays, so the next pass retries.
    (try
      (let [new-job (restart-job! job-name job diff)]
        (when-not (install-job! running job-name job new-job)
          (log/infof "job %s is gone, stopping the worker we just started" job-name)
          ((:stop! new-job))))
      (catch KafkaException e
        (log/errorf e "job %s could not be restarted, retrying on the next pass" job-name)))))


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

  ;; master first: otherwise it may restart a job we are stopping. install-job!
  ;; would stop that worker again, but only after a needless rebalance.
  (stop [this]
    (when-let [^Thread m @master]
      (.interrupt m)
      (.join m 5000))
    (doseq [job-name (keys @*running)]
      (stop this job-name)))

  ;; dissoc first, stop! after: same reason as check-jobs! — a function handed
  ;; to swap! must be pure. Of two racing stops only one sees the entry in
  ;; `before`; restart-job! may still call the same stop! from the master,
  ;; which is harmless — stop! can be called any number of times.
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
