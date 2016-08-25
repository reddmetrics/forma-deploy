(defproject forma-deploy/forma-deploy "0.1.0-SNAPSHOT" 
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/tools.cli "0.1.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [backtype/dfs-datastores-cascading "1.0.5"]]
  :profiles {:dev
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
               [pallet-hadoop "0.3.2"]
               [org.jclouds.provider/aws-ec2 "1.0.0"]
               [org.jclouds.driver/jclouds-jsch "1.0.0"]
               [org.jclouds.driver/jclouds-log4j "1.0.0"]
               [log4j/log4j "1.2.14"]
               [vmfest/vmfest "0.2.2"]]}}
  :main forma.hadoop.cluster
  :min-lein-version "2.0.0"
  :description "Hadoop deploy for FORMA.")
