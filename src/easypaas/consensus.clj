(ns easypaas.consensus
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [zookeeper.util :as util]))

(defn connect
  [{root :root}]
  (let [client (zk/connect "127.0.0.1:2181")]
    (when-not (zk/exists client root)
      (zk/create client root :persistent? true))
    {:client client
     :root root}))


(defn set-status
  [{:keys [root client]} status]
  (when-let [{:keys [version]} (zk/exists client (str root "/status"))]
    (zk/set-data client (str root "/status") (data/to-bytes (pr-str status)) version)))

(defn get-status
  [{:keys [root client]}]
  (-> (:data (zk/data client (str root "/status")))
      data/to-string
      read-string))

(defn init
  [job]
  (let [{:keys [client root]} (connect job)]
    (zk/delete-all client root)
    (zk/create client root :persistent? true)
    (zk/create client (str root "/election") :persistent? true)
    (zk/create client (str root "/status") :persistent? true :data (data/to-bytes (pr-str :running)))))


(defn node-from-path [{:keys [root]} path]
  (.substring path (inc (count (str root "/election")))))

(declare elect-leader)

(defn watch-predecessor [{:keys [client root leader-fn] :as context} me pred leader {:keys [event-type path]}]
  (if (and (= event-type :NodeDeleted) (= (node-from-path context path) leader))
    (leader-fn)
    (if-not (zk/exists client (str root "/election/" pred)
                       :watcher (partial watch-predecessor context me pred leader))
      (elect-leader client leader-fn me))))

(defn predecessor [me coll]
  (ffirst (filter #(= (second %) me) (partition 2 1 coll))))

(defn elect-leader [{:keys [client root leader-fn] :as context} me]
  (let [members (util/sort-sequential-nodes (zk/children client (str root "/election")))
        leader (first members)]
    (if (= me leader)
      (leader-fn)
      (let [pred (predecessor me members)]
        (if-not (zk/exists client (str root "/election/" pred)
                           :watcher (partial watch-predecessor context me pred leader))
          (elect-leader context me))))))

(defn watch-status [{:keys [client root] :as context} {:keys [event-type]}]
  (if (= event-type :NodeDataChanged)
    (let [{:keys [data]} (zk/data client (str root "/status"))
          status (read-string (data/to-string data))]
      (if (= status :shutdown)
        (System/exit 0)
        (zk/exists client (str root "/status")
                   :watcher (partial watch-status context))))
    (zk/exists client (str root "/status")
               :watcher (partial watch-status context))))

(defn join-group [{:keys [client root] :as context}]
  (let [me (node-from-path context (zk/create client (str root "/election/n-") :sequential? true))]
    (zk/exists client (str root "/status") :watcher (partial watch-status context))
    (elect-leader context me)))

(defn members [job]
  (let [{:keys [client root]} (connect job)
        children (zk/children client (str root "/election"))]
    (zk/close client)
    children))
