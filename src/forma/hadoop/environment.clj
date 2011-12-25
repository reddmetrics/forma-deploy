(ns forma.hadoop.environment
  (:use [pallet.phase :only (phase-fn)]
        [pallet.resource.package :only (package-manager)]
        [pallet.compute.vmfest :only (parallel-create-nodes)])
  (:require [pallet.compute :as compute]
            [pallet.core :as core])
  (:import [java.net InetAddress]))

;; ### EC2 Environment

(def remote-env
  {:algorithms {:lift-fn pallet.core/parallel-lift
                :converge-fn pallet.core/parallel-adjust-node-counts}})

(defn mk-ec2-service [] (compute/service :aws))

(defmacro with-ec2-service
  "Binds the result of (env/mk-ec2-service) to the supplied `sym`, and
  makes it available to all forms. If the ec2-service can't be
  generated, skips forms and throws an error."
  [[sym] & forms]
  `(if-let [~sym (mk-ec2-service)]
     (do ~@forms)
     (println "Sorry, there seems to be a problem with the ec2 service.")))

;; ### Local Environment

(defn mk-vm-service [] (compute/service :virtualbox))

(def parallel-env
  {:algorithms
   {:lift-fn core/parallel-lift
    :vmfest {:create-nodes-fn parallel-create-nodes}
    :converge-fn core/parallel-adjust-node-counts}})

(def local-proxy (format "http://%s:3128"
                         (.getHostAddress
                          (InetAddress/getLocalHost))))

(def local-node-specs
  (merge remote-env
         {:proxy local-proxy
          :phases {:bootstrap
                   (phase-fn
                    (package-manager
                     :configure :proxy local-proxy))}}))

(def vm-env
  (merge local-node-specs parallel-env))
