(local cjson (require :cjson))
(local inspect (require :inspect))
(local pl-set (require :pl.Set))
(local seq (require :pl.seq))

(cjson.encode_empty_table_as_object false)

(var node-topology nil)
(var node-id nil)
(var message-store (pl-set []))
(var pending-acks {})
(var acks {})

(fn contains? [message pset]
  (not= nil (. pset message)))

(fn reply [resp]
  (when (not= nil resp)
    (-> (cjson.encode resp)
        (print))))

(fn send-request [node body]
  (let [{: msg_id : message} body
        pending-ack-req {}]
    (do
      (tset pending-ack-req node message)
      (tset pending-acks msg_id pending-ack-req)
      (reply {:src node-id :dest node : body}))))

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
    (when (not (contains? message message-store))
      (set message-store (+ message-store message))
      (-> (seq.list neighbours)
          (seq.filter #(not= $1 dest-node))
          (seq.foreach #(send-request $1 body)))
      {:src node-id
       :dest dest-node
       :body {:msg_id (+ msg_id 1) :in_reply_to msg_id :type :broadcast_ok}})))

(fn handle-broadcast-ok [dest-node body]
  (let [{:in_reply_to msg_id : message} body
        ackd-msg {}]
    (tset ackd-msg dest-node message)
    (tset acks msg_id ackd-msg)))

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
          {: src : body : dest} input
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
        :broadcast_ok (do
                        (handle-broadcast-ok src body)
                        (io.stderr:write (.. "\nReceived ack for " dest
                                             " from " src)))
        :read (-> (handle-read src body)
                  (reply))))))

(main)
