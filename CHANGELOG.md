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