#!/usr/bin/env fennel

(local cjson (require :cjson))
(local inspect (require :inspect))
(local tablex (require :pl.tablex))

(fn handle-echo [node-id input]
  (let [{: src : body} input
        {: msg_id} body]
    {:src node-id
     :dest src
     :body (tablex.merge body {:msg_id (+ msg_id 1)
                               :in_reply_to msg_id
                               :type :echo_ok}
                         true)}))

(fn handle-init [node-id input]
  (let [{: src : body} input
        {: msg_id} body]
    {:src node-id
     :dest src
     :body {:msg_id (+ msg_id 1) :in_reply_to msg_id :type :init_ok}}))

(fn main []
  (var node-id nil)
  (while true
    (let [input (->> (io.read :*l)
                     (cjson.decode))
          {: body} input
          {: node_id : type} body]
      (when (= nil node-id)
        (set node-id node_id))
      (match type
        :init (do
                (io.stderr:write (.. "initialized node " node-id "\n"))
                (-> (handle-init node-id input)
                    (cjson.encode)
                    (print)))
        :echo (do
                (io.stderr:write (.. "Echoing body on node " node-id "\n"))
                (-> (handle-echo node-id input)
                    (cjson.encode)
                    (print)))))))

(main)
