(ns core
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [langohr.exchange  :as le]
            [clojure.tools.reader.edn :as edn]))

(def qname "test-queue")
(def ^{:const true} default-exchange "")
(def direct-exchange "my-direct-exchange")
(def topic-exchange "my-topic-exchange")

;; Create a queue
(let [conn (rmq/connect)
      ch (lch/open conn)]
  (lq/declare ch qname {:exclusive false :auto-delete false})
  (lch/close ch)
  (rmq/close conn))

;; Create a direct exchange
(let [conn (rmq/connect)
      ch (lch/open conn)]
  (le/declare ch direct-exchange "direct")
  (lq/bind ch qname direct-exchange {:routing-key "direct-route"})
  (lch/close ch)
  (rmq/close conn))

;; Create a topic exchange
(let [conn (rmq/connect)
      ch (lch/open conn)]
  (le/declare ch topic-exchange "topic")
  (lq/bind ch qname topic-exchange {:routing-key "topic.*"})
  (lch/close ch)
  (rmq/close conn))


;; Publish message to exchange
(let [conn (rmq/connect)
      ch (lch/open conn)
      msg {:data "this is my data from default"}]
  (lb/publish ch default-exchange qname (prn-str msg))
  (lch/close ch)
  (rmq/close conn))

;; Publish message to direct exchange
(let [conn (rmq/connect)
      ch (lch/open conn)
      msg {:data "this is my data from direct"}]
  (lb/publish ch direct-exchange "direct-route" (prn-str msg))
  (lch/close ch)
  (rmq/close conn))

;; Publish message to topic exchange
(let [conn (rmq/connect)
      ch (lch/open conn)
      msg {:data "this is my data from topic"}]
  (lb/publish ch topic-exchange "topic.anything" (prn-str msg))
  (lch/close ch)
  (rmq/close conn))


;; consume message from queue

(defn message-handler [ch meta payload]
  (let [parsed-payload (edn/read-string (String. payload "UTF-8"))]
    (println "payload received " (:data parsed-payload))))

(defn start-consumer []
  (let [conn (rmq/connect)
        ch (lch/open conn)]
    (lc/subscribe ch qname message-handler {:auto-ack true})))

(start-consumer)