#!/usr/bin/env fennel

(local cjson (require :cjson))
(local inspect (require :inspect))
(local tablex (require :pl.tablex))

(fn reply [node-id input resp]
  (let [{: src : body} input
        {: msg_id} body
        reply-body (tablex.merge body resp true)]
    (tset reply-body :msg_id (+ msg_id 1))
    (tset reply-body :in_reply_to msg_id)
    (tset input :src node-id)
    (tset input :dest src)
    (tset input :body reply-body)
    input))

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
                (-> (reply node-id input {:type :init_ok})
                    (cjson.encode)
                    (print)))
        :echo (do
                (io.stderr:write (.. "Echoing body on node " node-id "\n"))
                (-> node-id
                    (reply input (tablex.merge body {:type :echo_ok} true))
                    (cjson.encode)
                    (print)))))))

(main)
