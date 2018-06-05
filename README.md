# CRANK

Crank is a library for building job-processing systems. It's main goals are
simplicity and understandability.

**NOTE: tests are not working yet.**

## Limitations

- the only source for messages is kafka
- every job is just a single thread

## Usage

```
(require '[crank.core :as crank])

(def monitor (crank/init {:report prn})) ; :report is optional

(crank/start monitor "mk order"
  {:topic "mk_order"
   :func  prn
   :timeout 5000                ; optional
   :kafka {:uri   "kafka1:9192"
           :group "crank"
           :batch-size 10000}}) ; optional
```

## Description

Crank will fire up thread for a job, and will pass data read from Kafka to
supplied function. If a job dies because of something - an exception occured
inside job or inside Crank itself - Crank's monitor will attempt to restart the
job.

To stop job processing you can call `(crank/stop monitor)`. Crank supports
having multiple monitors, there is no shared state.
