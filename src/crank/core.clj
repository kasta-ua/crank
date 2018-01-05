(ns crank.core
  (:require [crank.kafka :as kafka]
            [crank.worker :as worker]))


(def kafka kafka/input)
(def start! worker/start)

