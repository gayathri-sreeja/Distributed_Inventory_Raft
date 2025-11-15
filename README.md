# Distributed_Inventory_Raft

A fault-tolerant, distributed inventory management system built using Python, gRPC, and a custom Raft consensus implementation.

This project ensures:

-> Strong consistency

-> Leader-based writes

-> Automatic leader election

-> Log replication

-> Fault recovery

-> Distributed inventory operations (read/write)

-> Optional LLM-assistant for customers & producers

The system runs 3 Raft nodes, each with:

-> A Raft server (for leader election & replication)

-> An Inventory gRPC server (for client operations)

Project Structure

Distributed_Inventory/
│
├── client/
│   └── client_app.py
│
├── server/
│   ├── main_server.py
│   ├── raft_node.py
│   ├── auth.py
│   ├── business_logic.py
│   └── data/
│       └── raft_n1.json / raft_n2.json / raft_n3.json
│
├── llm_server/
│   └── llm_server.py
│
├── proto/
│   ├── inventory.proto
│   ├── llm.proto
│   └── raft.proto
│
├── requirements.txt
└── README.md

High-Level System Architecture

1. Client (customer/producer)

    -> Requests inventory operations over gRPC

    -> Sends write requests only to the leader

    -> Can query LLM server for recommendations

2. Inventory Server (per node)

    -> Exposes operations:

    -> Login

    -> GetAllItems

    -> GetItem

    -> AddItem (producer only)

    -> UpdateItem (producer only)

    -> OrderItem (customer only)

3. Raft Server (per node)

    Implements:

        -> Leader election

        -> Heartbeats

        -> Log replication

        -> Commit & apply entries

        -> Recovery after restart

4. LLM Server

    -> Simple rule-based text assistant

    -> Helps customers/producers with item suggestions
How the System Works (Flow)

1. Client logs in

    Auth validated using JWT (auth.py).
    Returns token + role.

2. Client performs read operations

    All nodes can serve reads.

3. Client performs write operations

    Only leader accepts Add/Update/Order.

    Leader calls raft_node.propose().

    Log replicates to followers.

    Once majority ACKs → entry committed.

    apply_committed_log() updates inventory.

    Followers apply the same entry.

4. If leader dies

    Followers wait for timeout

    New leader is elected

    Logs remain consistent

    Old leader syncs missing logs on restart
Installation

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
Protobuf Compilation (if needed)

python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/inventory.proto
python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/raft.proto
python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/llm.proto
Running the System (3-node Raft Cluster)

Open 3 terminals.

Terminal 1 → Node n1
-> NODE_ID=n1 INV_PORT=50051 python -m server.main_server

Terminal 2 → Node n2
-> NODE_ID=n2 INV_PORT=50052 python -m server.main_server

Terminal 3 → Node n3
-> NODE_ID=n3 INV_PORT=50053 python -m server.main_server

You should see something like:
✅ RAFT server running on 6001
✅ Inventory server running on 50051
✅ Leader elected: n1 (term=23)
Running the LLM Server (Optional)

python -m llm_server.llm_server
Running the Client

python -m client.client_app
Input:

Inventory host [localhost]: localhost
Inventory port [50051/50052/50053]: 50051   (Leader’s port)
Then login:

Producer login:
    username: Sarthak
    password: password2

Customer login:
    username: Gayathri
    password: password1
Client Commands

Inside client shell:

1. Get all items

    cmd> get_all

2. Get one item

    cmd> get_item
    Enter item id: 3

3. Add new item (producer only)

    cmd> restock
    item_id (leave blank to add new):
    name: Book
    description: Book Paper
    quantity: 10
    price: 100

4. Update item stock

    cmd> restock
    item_id: 2
    quantity: 10
    price: 25000

5. Order item (customer only)

    cmd> get_item
    cmd> order

6. LLM help

    cmd> llm
    LLM query text: suggest me cheap items
Logout

 cmd> logout
Quit client

 cmd> exit
Milestone 2 Testing (Very Important)

Leader Election

Kill leader:

 ctrl + C on n1
Expected:

 Leader elected: n2 (term=XYZ)
Log Replication

Using client:

 restock → Add new item
Expected on ALL nodes:

 [RAFT APPLY] Added item 'Book' with id=6
Failure Detection

Kill a follower:

 Ctrl + C on n3
Leader & remaining follower still continue.

Recovery

Restart follower:

 NODE_ID=n3 INV_PORT=50053 python -m server.main_server
Expected:

 [RAFT APPLY] Added item 'Book' with id=6
 [RAFT APPLY] Added item 'Pen' with id=7
Troubleshooting

“Not leader”

Connect client to leader’s inventory port.
JSONDecodeError

Delete corrupted RAFT state:

rm data/raft_n1.json data/raft_n2.json data/raft_n3.json
Conclusion

This project demonstrates a fully working distributed system built from scratch, complete with:

    ->Distributed consensus

    ->Eventual consistency

    ->Fault tolerance

    ->Replicated state machine

    ->Authentication

    ->gRPC microservices

    ->Optional LLM integration
