(ns forma.hadoop.cli)

;; ## CLI Utilities

(defn flip [f]
  #(apply f (reverse %&)))

(defn has [n xs]
  (= n (count xs)))

(defn select-truthy-keys
  [m key-seq]
  (->> (select-keys m key-seq)
       (filter second)
       (into {})))

;; ## Validators

(defn add-error [m e-string]
  (update-in m [:_errors] conj e-string))

(defn print-errors
  [error-seq]
  (doseq [e error-seq]
    (println e)))

(defn just-one? [m & kwds]
  (let [entries (select-truthy-keys m kwds)]
    (cond (has 1 entries) m
          (has 0 entries)
          (add-error m (str "Please provide one of the following: " kwds))
          :else (add-error m (str "Only one of the following is allowed: "
                                  (keys entries))))))

(defmacro build-validator
  [& validators]
  `(fn [arg-map#]
     (-> arg-map# ~@validators)))

(defn name-present?
  "Example validation step. This step checks that, if destroy or
  provision exist in the arg map, they're accompanied by a name. If
  this passes, the function acts as identity, else an error is added
  to the map."
  [{:keys [destroy provision name] :as m}]
  (cond (and destroy (not name))   (add-error m "Destroy requires a name.")
        (and provision (not name)) (add-error m "Provision requires a name.")
        :else m))

(def ^{:doc "This is how you build the validator that's passed into
cli-interface."}
  name-validator
  (build-validator name-present?))

;; ## CLI Builder

(defn cli-interface
  [parser validator func]
  (fn [& args]
    (let [arg-map (-> args parser validator)]
      (if-let [e-seq (:_errors arg-map)]
        (v/print-errors e-seq)
        (func arg-map)))))
