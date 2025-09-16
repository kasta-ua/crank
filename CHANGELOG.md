## 2.0.0

### Breaking Changes
* **BREAKING**: Updated Kafka client from 2.6.0 to 4.1.0
* Now requires Java 8+ (Kafka 4.x uses `java.time.Duration` for timeouts)
* Projects using crank must also update to Kafka 4.x
* `.poll()` method now uses `Duration` instead of `long` for timeout parameter

## 1.2.8

* Fixed `job-name` being `nil` in `check-job` logs
* Consumer config accepts any consumer property

## 1.2.7

* Fixed ArityException introduced in 1.2.6

## 1.2.6

* `update-vals` is now in `clojure.core`

## 1.2.5

* `Thread/interrupt` is not exactly the best thing in the world

## 1.2.4

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
