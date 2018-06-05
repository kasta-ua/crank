(ns crank.worker-test
  (:require [clojure.test :as test :refer [deftest is]]
            [clojure.core.async :as a]

            [crank.worker :as worker]
            [crank.input.async :as input.a]))

;;; setup

(def ch1 (a/chan 10))
(def ch2 (a/chan 10))
(def ch3 (a/chan 10))
(def STORE (atom []))


(defn store-input [marker]
  (fn [segment]
    (swap! STORE conj [marker segment])))


(def JOBS
  {"test"          {:input (input.a/input {:ch ch1})
                    :func  (store-input 1)}
   "test-conflict" {:input (input.a/input {:ch ch1})
                    :func  (store-input 2)}
   "test3"         {:input (input.a/input {:ch ch3})
                    :func  (store-input 3)}})


;;; test cases

(deftest can-work
  (a/>!! ch1 "test")
  (a/>!! ch2 "test")
  (a/>!! ch3 "test")
  (let [[started rest] (worker/start 2 JOBS)]
    (is (= ["test" "test3"] (keys started)))
    ;;(is (= ["test3"] (map first rest)))
    (is (= (into #{} @STORE)
           #{[1 "test"]
             [3 "test"]}))))


;;; utility

(defn cleanup-store [f]
  (reset! STORE [])
  (f))


(test/use-fixtures :each cleanup-store)


(defn -main [& args]
  (test/run-tests 'crank.worker-test))
