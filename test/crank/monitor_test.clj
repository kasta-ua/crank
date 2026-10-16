(ns crank.monitor-test
  "Monitor-loop tests. No Kafka on purpose: job/start-job is stubbed out, so
  these stay fast and deterministic."
  (:require [clojure.test :as test :refer [deftest is testing]]

            [crank.core :as crank]
            [crank.job :as job])
  (:import [org.apache.kafka.common KafkaException]))


(defn- fake-worker [name]
  (Thread. ^Runnable (fn []) ^String name))


(defn- fake-job
  "A job entry shaped the way crank.job/start-job builds one, after its worker
  died. silent-ms is how long ago it last reported. Like the real :exception
  report, this one has no :attempts — only :start carries it."
  [{:keys [job-name silent-ms stops]
    :or   {silent-ms 0}}]
  {:config {:job-name job-name :timeout 1000}
   :worker (fake-worker (str "fake-" job-name))
   :report [{:type     :exception
             :job-name job-name
             :time     (- (System/currentTimeMillis) silent-ms)}]
   :stop!  #(swap! stops conj job-name)})


(defn- restarted-job
  "What the stubbed start-job hands back."
  [config stops]
  {:config config
   :worker (fake-worker "fake-restarted")
   :report []
   :stop!  #(swap! stops conj "restarted")})


(deftest job-timeout-test
  (let [stops (atom [])]
    (testing "a job that reported just now is alive"
      (is (nil? (crank/job-timeout (fake-job {:job-name "j" :stops stops})))))

    (testing "a job silent past its timeout reports how long it has been silent"
      (is (<= 5000 (crank/job-timeout (fake-job {:job-name  "j"
                                                 :silent-ms 5000
                                                 :stops     stops})))))

    (testing "the timeout doubles with every attempt of the latest :start"
      ;; 1000 * 2^3 = 8000, so 5 seconds of silence is still within budget
      (is (nil? (crank/job-timeout
                  (assoc (fake-job {:job-name "j" :stops stops})
                    :report [{:type     :start
                              :attempts 3
                              :time     (- (System/currentTimeMillis) 5000)}])))))))


(deftest restart-happens-exactly-once
  (testing "a job is restarted once even when *running keeps changing underneath"
    (let [stops   (atom [])
          starts  (atom 0)
          running (atom nil)]
      (reset! running
        {"dead"  (fake-job {:job-name "dead" :silent-ms 5000 :stops stops})
         "alive" (fake-job {:job-name "alive" :stops stops})})

      (with-redefs [job/start-job
                    (fn [config]
                      ;; A live job reporting in the middle of a restart. This
                      ;; is the regression: while the restart lived inside
                      ;; swap!, such a write invalidated the CAS, swap! reran
                      ;; the whole thing and start-job spawned another consumer
                      ;; on every retry — orphaned, since only the last map
                      ;; would ever be installed.
                      ;;
                      ;; Only the first call perturbs the atom: otherwise the
                      ;; old code never converges and the test hangs instead of
                      ;; failing.
                      (when (zero? @starts)
                        (swap! running update "alive" crank/add-report
                          {:type :message :time (System/currentTimeMillis)}))
                      (swap! starts inc)
                      (restarted-job config stops))]
        (crank/check-jobs! running))

      (is (= 1 @starts)
        "exactly one worker started")
      (is (= ["dead"] @stops)
        "only the dead job was asked to stop")
      (is (= "fake-restarted" (.getName ^Thread (:worker (get @running "dead"))))
        "the monitor now tracks the new worker, not the dead one"))))


(deftest fresh-worker-reporting-start-is-kept
  (testing "the :start of the new worker landing before it is installed does not count as the job gone"
    (let [stops   (atom [])
          running (atom nil)]
      (reset! running
        {"dead" (fake-job {:job-name "dead" :silent-ms 5000 :stops stops})})

      (with-redefs [job/start-job
                    (fn [config]
                      ;; what crank.job/run-loop does first thing, through
                      ;; send-report — into the entry still holding the old job
                      (swap! running update "dead" crank/add-report
                        {:type :start :time (System/currentTimeMillis)})
                      (restarted-job config stops))]
        (crank/check-jobs! running))

      (is (= ["dead"] @stops)
        "the fresh worker is not stopped")
      (is (= "fake-restarted" (.getName ^Thread (:worker (get @running "dead"))))
        "and is the one the monitor tracks"))))


(deftest vanished-job-does-not-leave-a-worker-behind
  (testing "a job stopped mid-restart gets its fresh worker stopped, not orphaned"
    (let [stops   (atom [])
          running (atom nil)]
      (reset! running
        {"dead" (fake-job {:job-name "dead" :silent-ms 5000 :stops stops})})

      (with-redefs [job/start-job
                    (fn [config]
                      ;; Monitor.stop raced us and dropped the job
                      (swap! running dissoc "dead")
                      (restarted-job config stops))]
        (crank/check-jobs! running))

      (is (= ["dead" "restarted"] @stops)
        "the worker we started for a job that no longer exists is stopped")
      (is (empty? @running)
        "and the stopped job is not resurrected"))))


(deftest failed-restart-is-retried
  (testing "a consumer that cannot be built does not kill the monitor"
    (let [stops   (atom [])
          running (atom nil)
          dead    (fake-job {:job-name "dead" :silent-ms 5000 :stops stops})]
      (reset! running {"dead" dead})

      (with-redefs [job/start-job (fn [_] (throw (KafkaException. "no resolvable bootstrap urls")))]
        (crank/check-jobs! running))

      (is (identical? dead (get @running "dead"))
        "the entry stays as it was, so the next pass retries the restart"))))


(deftest live-jobs-are-left-alone
  (testing "nothing is restarted while every job keeps reporting"
    (let [stops   (atom [])
          starts  (atom 0)
          running (atom nil)]
      (reset! running
        {"a" (fake-job {:job-name "a" :stops stops})
         "b" (fake-job {:job-name "b" :silent-ms 500 :stops stops})})

      (with-redefs [job/start-job (fn [config]
                                    (swap! starts inc)
                                    (restarted-job config stops))]
        (crank/check-jobs! running))

      (is (zero? @starts))
      (is (empty? @stops)))))
