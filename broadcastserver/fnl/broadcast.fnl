(local cjson (require :cjson))
(local inspect (require :inspect))
(local pl-set (require :pl.Set))

(cjson.encode_empty_table_as_object false)

(var node-topology nil)
(var node-id nil)
(var message-store (pl-set []))

(fn reply [resp]
  (when (not= nil resp)
    (-> (cjson.encode resp)
        (print))))

(fn send-request [node body]
  (-> {:src node-id :dest node : body}
      (reply)))

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

(fn handle-broadcast [dest-node body]
  (let [{: msg_id : message} body
        neighbours (. node-topology node-id)]
    (when (= nil (. message-store message))
      (set message-store (+ message-store message))
      (each [_ neighbour-node (ipairs neighbours)]
        (when (not= neighbour-node dest-node)
          (send-request neighbour-node {:type :broadcast : message}))))
    (when (not= nil msg_id)
      {:src node-id
       :dest dest-node
       :body {:msg_id (+ msg_id 1) :in_reply_to msg_id :type :broadcast_ok}})))

(fn handle-read [dest-node body]
  (let [{: msg_id} body]
    {:src node-id
     :dest dest-node
     :body {:msg_id (+ msg_id 1)
            :in_reply_to msg_id
            :messages (pl-set.values message-store)
            :type :read_ok}}))

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
                  (reply))
        :topology (-> (handle-topology src body)
                      (reply))
        :broadcast (-> (handle-broadcast src body)
                       (reply))
        :read (-> (handle-read src body)
                  (reply))))))

(main)
