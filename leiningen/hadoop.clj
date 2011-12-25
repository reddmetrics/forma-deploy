(ns leiningen.hadoop
  (:use [leiningen.core :only (abort)]
        [leiningen.compile :only (eval-in-project)]
        [leiningen.jar :only (get-jar-filename get-default-uberjar-name)]
        [leiningen.uberjar :only (uberjar)]))

(defn execute-jar [project node-type & args]
  (let [uberjar-name (get-default-uberjar-name project)
        standalone-filepath (get-jar-filename project uberjar-name)
        dest-path (str "/home/hadoop/" uberjar-name)]
    (println (str "Uberjarring project to " uberjar-name))
    (uberjar project)
    (println "Uberjarring complete. Preparing to upload jarfile to jobtracker.")
    (eval-in-project
     project
     `(let [master-dns# (forma.hadoop.cluster/jobtracker-ip ~node-type)]
        (println "uploading jar now...")
        (forma.hadoop.cluster/scp-uberjar ~standalone-filepath
                                          ~dest-path
                                          keypath#
                                          master-dns#)
        (println "jar uploaded. Running hadoop commands.")
        (pallet.core/lift (forma.hadoop.cluster/master-nodeset ~node-type)
                          :phase (pallet.phase/phase-fn
                                  (pallet.crate.hadoop/hadoop-command
                                   "jar" ~dest-path ~@args))
                          :compute forma.hadoop.environment/ec2-service
                          :environment forma.hadoop.environment/remote-env)
        (println "Your job's complete. Kill with Ctrl-C."))
     nil nil
     `(do (require 'forma.hadoop.cluster)
          (require 'forma.hadoop.environment)
          (require 'pallet.crate.hadoop)
          (require 'pallet.phase)
          (require 'pallet.core)))))

(defn hadoop
  "Executes the current uberjar onto the supplied hadoop cluster."
  [project & args]
  (if-not args
    (abort "Please provide hadoop arguments.")
    (apply execute-jar project args)))
