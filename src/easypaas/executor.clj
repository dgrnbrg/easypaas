(ns easypaas.executor
  (:require [clj-mesos
             [executor :as executor]]
            [easypaas
             [consensus :as consensus]
             [core :as core]] 
            [zookeeper :as zk]))

