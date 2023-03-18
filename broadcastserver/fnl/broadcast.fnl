(local cjson (require :cjson))
(local inspect (require :inspect))

(var node-topology nil)
(var node-id nil)

(fn handle-topology [dest-node body]
  (io.stderr:write "\nProcessing topology")
  (let [{: msg_id : topology} body]
    (set node-topology topology)
    {:src node-id
     :dest dest-node
     :body {:msg_id (+ msg_id 1) :in_reply_to msg_id :type :topology_ok}}))

(fn handle-init [dest-node body]
  (io.stderr:write (.. "\nInitialzing node " node-id))
  (let [{: msg_id} body]
    {:src node-id
     :dest dest-node
     :body {:msg_id (+ msg_id 1) :in_reply_to msg_id :type :init_ok}}))

(fn main []
  (while true
    (let [input (->> (io.read :*l)
                     (cjson.decode))
          {: src : body} input
          {: node_id : type} body]
      (when (= nil node-id)
        (set node-id node_id))
      (match type
        :init (-> (handle-init src body)
                  (cjson.encode)
                  (print))
        :topology (-> (handle-topology src body)
                      (cjson.encode)
                      (print))))))

(main)
