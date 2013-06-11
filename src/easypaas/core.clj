(ns easypaas.core
  (:require [clj-mesos
             [scheduler :as scheduler]
             [executor :as executor]]
            [me.raynes.conch.low-level :as sh]
            [zookeeper :as zk]
            [zookeeper.data :as data]
            [easypaas.consensus :as consensus]))

(def job-blah {:command ["/bin/sh" "-c" "sleep $[ ( $RANDOM % 600 ) + 30 ]"]
               :name "random-sleeper"
               :root "/random-sleeper"
               :target {:instances 10
                        :machines 1}
               :resources {:cpus 0.25
                           :mem 10.0}})

(defn allocate-resources-to-job
  [driver failover-cfg job offers]
  ;;TODO: support multi-machine goal
  (let [{{cpus-req :cpus mem-req :mem} :resources
         {target-instances :instances target-machines :machines} :target
         job-name :name
         :keys [command]} job
        current-instances (count (consensus/members job))
        needed-instances (max (- target-instances current-instances) 0)]
    (.println System/out (str "need " needed-instances " instances, have " current-instances " instances"))
    (reduce
      (fn [[needed-instances remaining-offers] offer]
        (let [{:keys [id slave-id]
               {:keys [cpus mem]} :resources} offer]
          (if (and (pos? needed-instances) (>= cpus cpus-req) (>= mem mem-req))
            (let [max-by-cpus (int (/ cpus cpus-req))
                  max-by-mem (int (/ mem mem-req))
                  max-possible (min max-by-cpus max-by-mem needed-instances)
                  uuids (repeatedly max-possible #(str (java.util.UUID/randomUUID)))
                  remaining-cpus (- cpus (* max-possible cpus-req))
                  remaining-mem (- mem (* max-possible mem-req))]
              (.println System/out (str "about to launch tasks:" max-possible))
              (scheduler/launch-tasks driver id
                                      (map (fn [uuid]
                                             {:name job-name
                                              :task-id uuid
                                              :slave-id slave-id
                                              :resources {:cpus cpus-req :mem mem-req}
                                              :executor {:executor-id uuid
                                                         :framework-id (-> failover-cfg :framework-info :id)
                                                         :command {:value "/Users/dgrnbrg/easypaas/executor"}
                                                         :data (data/to-bytes (pr-str failover-cfg))}
                                              :data (data/to-bytes (pr-str job))})
                                           uuids))
              [(- needed-instances max-possible)
               (conj remaining-offers
                     (-> offer
                         (assoc-in [:resources :cpus] remaining-cpus) 
                         (assoc-in [:resources :mem] remaining-mem)))])
            [needed-instances (conj remaining-offers offer)])))
      [needed-instances []]
      offers)))

(defn make-scheduler
  [job master framework-info]
  (let [framework (promise)
        sched (scheduler/scheduler
                (registered [driver framework-id master-info]
                            (deliver framework (assoc framework-info :id framework-id)))
                (reregistered [driver master-info])
                (resourceOffers [driver offers]
                                (allocate-resources-to-job
                                  driver
                                  {:master master :framework-info @framework}
                                  job
                                  offers))
                (statusUpdate [driver status]
                              (when (#{:task-finished
                                       :task-failed
                                       :task-killed
                                       :task-lost} (:state status))
                                #_(.requestResources driver [(clj-mesos.marshalling/map->proto org.apache.mesos.Protos$Request {:resources {:cpus 0.25
                                                                                    :mem 10.0}})])))
                (disconnected [driver]
                       (.println System/out (str "disco!!!")))
                (slaveLost [driver slave-id])
                (executorLost [driver executor-id slave-id status])
                (error [driver message]
                       (.println System/out (str "error: " message))))]
    (scheduler/driver sched framework-info master)))

(let [master (promise)]
  (def exec
  (executor/executor
    (registered [driver executor-info framework-info slave-info]
                (println "Got framework info:")
                (deliver master (-> executor-info
                                   :data
                                   data/to-string
                                   read-string))
                (clojure.pprint/pprint @master))
    (reregistered [driver slave-info])
    (disconnected [driver])
    (launchTask [driver task]
                (executor/send-status-update driver {:task-id (:task-id task)
                                                     :state :task-starting})
                (let [job (read-string (data/to-string (:data task)))
                      context (consensus/connect job)
                      _ (consensus/join-group
                          (assoc context
                                 :leader-fn #_(fn [& _] (println "leading"))
                                 #(let [sched (make-scheduler
                                                           job
                                                           (:master @master)
                                                           (:framework-info @master))]
                                               (scheduler/start sched)
                                               (println "I am the leader!"))))
                      task-args (:command job)
                      _ (println "Running task" task-args)
                      {:keys [out err] :as proc} (apply sh/proc task-args)]
                  (executor/send-status-update driver {:task-id (:task-id task)
                                                       :state :task-running})
                  (future
                    (doseq [line (line-seq out)]
                      (.println System/out line)))
                  (future
                    (doseq [line (line-seq err)]
                      (.println System/err line)))
                  (future
                    (let [exit-code (sh/exit-code proc)]
                      (println "Task finished with exit code" exit-code)
                      (executor/send-status-update driver {:task-id (:task-id task)
                                                           :state (if (zero? exit-code)
                                                                    :task-finished
                                                                    :task-failed)})
                      (System/exit exit-code)))))
    (killTask [driver task-id])
    (frameworkMessage [driver data])
    (shutdown [driver])
    (error [driver message]))))

(defn -main
  [command & args]
  (case command
    "executor"
    (do (println "entering executor")
        (-> exec
            (executor/driver)
            (executor/start)))))

(comment

  (def driver (make-scheduler job-blah "172.31.235.253:5050" {:user "" :name "easypaas"}))

  (do
    (consensus/init job-blah)
    (scheduler/start driver))

  (let [ctx (consensus/connect job-blah)]
    (println (consensus/get-status ctx))
    (zk/close (:client ctx)))

  (scheduler/stop driver)

  (println "n ="(count @counts))
  (println "min =" (quot (* 5 (count @counts)) 60))
  (println "mean ="(float (/ (apply + @counts) (count @counts))))
  (println "stddev ="(Math/sqrt (- (float (/ (apply + (map #(* % %) @counts)) (count @counts))) (#(* % %) (float (/ (apply + @counts) (count @counts)))) )))

  ;; Drop the first 5 minutes of data
  (swap! counts (partial drop (* 5 (/ 60000 5000))))

  (def counts (atom []))

  (future (doall (repeatedly #(do (Thread/sleep 5000) (swap! counts conj (count (consensus/members job-blah)))))))

  )
