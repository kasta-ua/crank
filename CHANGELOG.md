* `:message` and `:batch` reports now contain `:duration` of a call

## 1.2.3

* Report more data when job is considered dead

## 1.2.2

* Kafka client version update
* Stop consumer immediately if job fails
* Don't leak job if there is no report yet

## 1.2.1

* Control batch size in bytes via `:batch-bytes` option

## 1.2.0

* Feature: job timeout rises up to 10 minutes on each unsuccessfull attempt (by
  (number of retries)^2)
* Change: job loop logic around stopping job reworked a bit

## 1.1.0

* Feature: batch processing

## 1.0.1

* Fix: master thread is a daemon thread (should not prevent JVM from exiting)

## 1.0.0

* Initial release
