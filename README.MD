# Distributed Key-value Store with Data Partitioning

A DHT-based peer-to-peer key-value storage inspired by Amazon Dynamo.

The system consists of multiple storage nodes and provides a simple user interface to upload/request data and issue management commands. The stored data is partitioned among the nodes based on the value of keys identifying both the stored items and the nodes. Keys form a circular space or "ring", i.e. the largest key value wraps around to the smallest key value like minutes on analog clocks. A node is responsible for storing data items with keys that are less or equal than its own key but greater than the key of its counter-clockwise neighbour.

The interface for data operations consists of two commands: update(key, value) and get(key)->value. Any node in the network must be able to fulfil both requests regardless of the key, forwarding data to/from appropriate node(s) if required.

In order to tolerate faults the system relies on replication. In addition to storing the item at node A, it gets replicated to A's next N-1 clockwise neighbours (N nodes in total). When a node joins or leaves, the system moves data items accordingly. There are two system-wide parameters, W and R, specifying the write and read quorums respectively (W + R > N); a version number is associated internally with every stored item.

To make the system highly responsive, read and write requests are sent to the replicas in parallel (i.e. using separate threads) and the user gets the reply immediately after the first Q (or R) nodes reply in case of update (or get) operation, or when a timeout T triggers.

## Authors

* Astegher Maurizio
* Gambi Enrico