(ns forma.hadoop.cluster
  (:use forma.hadoop.cli
        clojure.tools.cli
        [clojure.string :only (join)]
        [pallet-hadoop.node :exclude (jobtracker-ip)]
        [pallet.crate.hadoop :only (hadoop-user)]        
        [pallet.extensions :only (def-phase-fn phase)])
  (:require [pallet.execute :as execute]
            [forma.hadoop.environment :as env]
            [pallet.script :as script]
            [pallet.stevedore :as stevedore]    
            [pallet.action.exec-script :as exec-script]
            [pallet.resource.remote-directory :as rd]
            [pallet.resource.directory :as d]
            [pallet.resource.package :as package])
  (:import [cascading.tuple.hadoop BytesSerialization TupleSerialization]
           [org.apache.hadoop.io.serializer WritableSerialization JavaSerialization]))

;; ### Job Run

(def-phase-fn install-redd-configs
  "Takes pairs of strings -- the first should be a filename in the
  `reddconfig` bucket on s3, and the second should be the unpacking
  directory on the node."
  [& filename-localpath-pairs]
  (for [[remote local] (partition 2 filename-localpath-pairs)]
    (rd/remote-directory local
                         :url (str "https://reddconfig.s3.amazonaws.com/" remote)
                         :unpack :tar
                         :tar-options "xz"
                         :strip-components 2
                         :owner hadoop-user
                         :group hadoop-user)))

(def redd-config-path "s3://reddconfig/bootstrap-actions/config.xml")
(def fw-path "/usr/local/fwtools")
(def native-path "/home/hadoop/native")

(def tokens
  (join "," ["130=forma.schema.IntArray"
             "131=forma.schema.DoubleArray"
             "132=forma.schema.FireTuple"
             "133=forma.schema.FireSeries"
             "134=forma.schema.FormaValue"
             "135=forma.schema.FormaNeighborValue"
             "136=forma.schema.FormaSeries"]))

(def-phase-fn config-redd
  "This phase installs the two files that we need to make redd run
  with gdal! We also change the permissions on `/mnt` to allow for
  some heavy duty storage space. We do need some better documentation,
  here."
  []
  (d/directory "/mnt"
               :owner hadoop-user
               :group hadoop-user
               :mode "0755")
  (package/package "libhdf4-dev")
  (install-redd-configs
   "FWTools-linux-x86_64-4.0.0.tar.gz" fw-path
   "linuxnative.tar.gz" native-path))

(def-phase-fn raise-file-limits
  "Raises file descriptor limits on all machines to the specified
  limit."
  [lim]
  (let [command  #(join " " [hadoop-user % "nofile" lim])
        path "/etc/security/limits.conf"
        pam-lims "session required  pam_limits.so"]
    (exec-script/exec-script
     ((echo ~(command "hard")) ">>" ~path))
    (exec-script/exec-script
     ((echo ~(command "soft")) ">>" ~path))
    (exec-script/exec-script
     ((echo ~pam-lims) ">>" "/etc/pam.d/common-session"))))

(defn mk-profile
  [mappers reducers spot-price ami hardware-id]
  {:map-tasks mappers
   :reduce-tasks reducers
   :image-id ami
   :hardware-id hardware-id
   :price spot-price})

(def cluster-profiles
  {"large"           (mk-profile 4 2 1.20 "us-east-1/ami-08f40561" "m1.large")
   "high-memory"     (mk-profile 30 24 1.50 "us-east-1/ami-08f40561" "m2.4xlarge")
   "cluster-compute" (mk-profile 22 16 1.20 "us-east-1/ami-1cad5275" "cc1.4xlarge")})

(defn forma-cluster
  "Generates a FORMA cluster with the supplied number of nodes. We
  pick that reduce capacity based on the recommended 1.2 times the
  number of tasks times number of nodes."
  [cluster-key nodecount]
  {:pre [(not (nil? cluster-key))]}
  (let [lib-path (str fw-path "/usr/lib")
        {:keys [map-tasks reduce-tasks image-id hardware-id price]}
        (cluster-profiles (or cluster-key "high-memory"))]
    (cluster-spec
     :private
     {:jobtracker (node-group [:jobtracker :namenode])
      :slaves     (slave-group nodecount :spec {:spot-price (float price)})}
     :base-machine-spec {:hardware-id hardware-id
                         :image-id image-id}
     :base-props {:hadoop-env {:JAVA_LIBRARY_PATH native-path
                               :LD_LIBRARY_PATH lib-path}
                  :hdfs-site {:dfs.data.dir "/mnt/dfs/data"
                              :dfs.name.dir "/mnt/dfs/name"
                              :dfs.datanode.max.xcievers 5096
                              :dfs.namenode.handler.count 20
                              :dfs.block.size 134217728
                              :dfs.support.append true}
                  :core-site {:cascading.serialization.tokens tokens
                              :fs.s3n.awsAccessKeyId "AKIAJ56QWQ45GBJELGQA"
                              :fs.s3n.awsSecretAccessKey
                              "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"}
                  :mapred-site {:io.sort.mb 200
                                :io.sort.factor 40
                                :mapred.reduce.parallel.copies 20
                                :mapred.local.dir "/mnt/hadoop/mapred/local"
                                :mapred.task.timeout 10000000
                                :mapred.reduce.tasks (int (* reduce-tasks nodecount))
                                :mapred.tasktracker.map.tasks.maximum map-tasks
                                :mapred.tasktracker.reduce.tasks.maximum reduce-tasks
                                :mapred.reduce.max.attempts 12
                                :mapred.map.max.attempts 20
                                :mapred.job.reuse.jvm.num.tasks 20
                                :mapred.map.tasks.speculative.execution false
                                :mapred.reduce.tasks.speculative.execution false
                                :mapred.output.direct.NativeS3FileSystem true
                                :mapred.child.java.opts (str "-Djava.library.path="
                                                             native-path
                                                             " -Xms1024m -Xmx2048m")
                                :mapred.child.env (str "LD_LIBRARY_PATH="
                                                       lib-path)}})))

(defn jobtracker-ip
  [node-type]
  (env/with-ec2-service [service]
    (let [{defs :nodedefs} (forma-cluster node-type 0)
          [jt-tag] (roles->tags [:jobtracker] defs)]
      (master-ip service jt-tag :public))))

(defn master-nodeset [node-type]
  (let [cluster (forma-cluster node-type 0)
        [jt-tag] (roles->tags [:jobtracker] (:nodedefs cluster))]
    (some #(when (= jt-tag (:group-name %)) %)
          (cluster->node-set cluster))))

(defn scp-uberjar
  [standalone-filepath dest-path ip-or-dns]
  (execute/local-script
   (scp ~standalone-filepath ~(str ip-or-dns ":" dest-path))))

;; Lein Run functions!

(defn print-jobtracker-ip
  [node-type]
  (println (if-let [ip (jobtracker-ip node-type)]
             (format "The current jobtracker IP is %s." ip)
             "Sorry, no cluster seems to be running at the moment."))
  (println "Hit Ctrl-C to exit."))

(defn create-cluster!
  [node-type node-count]
  (env/with-ec2-service [service]
    (let [cluster (forma-cluster node-type node-count)]
      (println
       (format "Creating cluster of %s instances and %d nodes."
               node-type node-count))
      (boot-cluster cluster
                    :compute service
                    :environment env/remote-env)
      (lift-cluster cluster
                    :phase (phase config-redd (raise-file-limits 100000))
                    :compute service
                    :environment env/remote-env)
      (start-cluster cluster
                     :compute service
                     :environment env/remote-env)
      (println "Cluster created!")
      (println "Hit Ctrl-C to exit."))))

(defn destroy-cluster!
  [node-type]
  (env/with-ec2-service [service]
    (println "Destroying cluster.")
    (kill-cluster (forma-cluster node-type 0)
                  :compute service
                  :environment env/remote-env)
    (println "Cluster destroyed!")
    (println "Hit Ctrl-C to exit.")))

(defn parse-emr-config
  [conf-map]
  (let [bad-set #{:io.serializations
                  :cascading.serialization.tokens
                  :fs.s3n.awsAccessKeyId
                  :fs.s3n.awsSecretAccessKey
                  :dfs.data.dir
                  :dfs.name.dir
                  :mapred.local.dir}]
    (->> (mapcat conf-map [:mapred-site :core-site :hdfs-site])
         (map (fn [[k v]]
                (when-not (bad-set k)
                  (format "-s,%s=%s" (name k) v))))
         (filter identity)
         (join ",")
         (format "\"--core-config-file,%s,%s\"" redd-config-path))))

(defn boot-emr!
  "TODO: Fix the way we get spot-price here."
  [node-type node-count name]
  (let [{:keys [base-props base-machine-spec]} (forma-cluster node-type node-count)
        {type :hardware-id} base-machine-spec]
    (execute/local-script
     (elastic-mapreduce --create --alive
                        --name ~(str "forma-" name)
                        --instance-group master
                        --instance-type ~type
                        --instance-count 1
                        --availability-zone us-east-1e
                        
                        --instance-group core
                        --instance-type ~type
                        --instance-count ~node-count
                        --bid-price  1.20 ;;~(:spot-price base-machine-spec)
                        --enable-debugging

                        --bootstrap-action
                        s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive
                        
                        --bootstrap-action
                        s3://elasticmapreduce/bootstrap-actions/add-swap
                        --args 2048
                        
                        --bootstrap-action
                        s3://elasticmapreduce/bootstrap-actions/configure-hadoop
                        --args ~(parse-emr-config base-props)

                        --bootstrap-action
                        s3://reddconfig/bootstrap-actions/forma_bootstrap_robin.sh))))

(defn parse-hadoop-args [args]
  (cli args
       (optional ["-n" "--name" "Name of cluster." :default "dev"])
       (optional ["-t" "--type" "Type  cluster." :default "high-memory"])
       (optional ["-s" "--size" "Size of cluster."] #(Long. %))
       (optional ["--jobtracker-ip" "Print jobtracker IP address?"])
       (optional ["--start" "Boots a Pallet cluster."])
       (optional ["--emr" "Boots an EMR cluster."])
       (optional ["--stop" "Kills a pallet cluster."])))

(defn size-present?
  "This step checks that, if `start` or `emr` exist in the arg map,
  they're accompanied by a size. If this passes, the function acts as
  identity, else an error is added to the map."
  [{:keys [start emr size] :as m}]
  (cond (and start (not size)) (add-error m "Start requires a name.")
        (and emr   (not size)) (add-error m "EMR requires a name.")
        :else m))

(def hadoop-validator
  (build-validator
   (just-one? :start :stop :emr :jobtracker-ip)
   (size-present?)))

(def -main
  (cli-interface parse-hadoop-args
                 hadoop-validator
                 (fn [{:keys [name type size] :as m}]
                   (condp (flip get) m
                     :start (create-cluster! type size)
                     :emr   (boot-emr! type size name)
                     :stop  (destroy-cluster! type)
                     :jobtracker-ip (print-jobtracker-ip type)
                     (println "Please provide an option!")))))
