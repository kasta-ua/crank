(ns crank.input)

(defprotocol Input
  (acquire [this])
  (receive [this timeout])
  (ack [this message]))
