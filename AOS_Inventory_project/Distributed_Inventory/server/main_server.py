# server/main_server.py
import os
import grpc
from concurrent import futures
import json
import time

# Auth + Raft
from server import auth
from server.raft_node import RaftNode

from server import inventory_pb2, inventory_pb2_grpc
from proto import raft_pb2, raft_pb2_grpc

# =========================================================
#                      RAFT CONFIG
# =========================================================

NODE_ID = os.getenv("NODE_ID", "n1")  # n1, n2, n3

# ----------- AUTO COMPUTE RAFT PORT ----------------
RAFT_PORT = 6000 + int(NODE_ID[1:])

# ----------- AUTO DISCOVER PEERS --------------------
def autodiscover_peers(my_id):
    peers = {}
    for i in range(1, 4):  # supports n1, n2, n3
        nid = f"n{i}"
        if nid == my_id:
            continue
        peers[nid] = f"localhost:{6000 + i}"
    return peers

RAFT_PEERS = autodiscover_peers(NODE_ID)

# ----------- AUTO INVENTORY PORT --------------------
INV_PORT = int(os.getenv("INV_PORT", 50050 + int(NODE_ID[1:])))

# =========================================================
#                IN-MEMORY INVENTORY
# =========================================================

inventory_data = {
    1: {"name": "Laptop", "description": "Portable PC", "quantity": 10, "price": 75000, "sales_history": []},
    2: {"name": "Smartphone", "description": "Android phone", "quantity": 25, "price": 30000, "sales_history": []},
    3: {"name": "Wireless Mouse", "description": "Optical mouse", "quantity": 50, "price": 800, "sales_history": []},
    4: {"name": "Headphones", "description": "Noise-cancelling", "quantity": 15, "price": 2500, "sales_history": []},
    5: {"name": "Keyboard", "description": "Mechanical keyboard", "quantity": 20, "price": 1500, "sales_history": []},
}

# =========================================================
#             APPLY COMMITTED RAFT LOG
# =========================================================

def apply_committed_log(entry):
    global inventory_data
    raw = entry["command_bytes"]
    if isinstance(raw, bytes):
        cmd = json.loads(raw.decode())
    else:
        cmd = json.loads(raw)

    ctype = entry["command_type"]

    # ---------- AddItem ----------
    if ctype == "AddItem":
        new_id = max(inventory_data.keys(), default=0) + 1
        inventory_data[new_id] = {
            "name": cmd["name"],
            "description": cmd["description"],
            "quantity": cmd["quantity"],
            "price": cmd["price"],
            "sales_history": []
        }
        print(f"[RAFT APPLY] Added item '{cmd['name']}' (id={new_id})")

    # ---------- UpdateItem ----------
    elif ctype == "UpdateItem":
        iid = int(cmd["id"])
        if iid in inventory_data:
            inventory_data[iid]["quantity"] += cmd["quantity"]
            inventory_data[iid]["price"] = cmd["price"]
            print(f"[RAFT APPLY] Updated item id={iid}")

    # ---------- OrderItem ----------
    elif ctype == "OrderItem":
        iid = int(cmd["id"])
        qty = int(cmd["quantity"])
        if iid in inventory_data and inventory_data[iid]["quantity"] >= qty:
            inventory_data[iid]["quantity"] -= qty
            # ---------- Track sales ----------
            if "sales_history" not in inventory_data[iid]:
                inventory_data[iid]["sales_history"] = []
            inventory_data[iid]["sales_history"].append(qty)
            print(f"[RAFT APPLY] Ordered id={iid} qty={qty}")

    # ---------- DeleteItem ----------
    elif ctype == "DeleteItem":
        iid = int(cmd["id"])
        if iid in inventory_data:
            del inventory_data[iid]
            print(f"[RAFT APPLY] Deleted item id={iid}")

# =========================================================
#              GRPC HELPER WITH RETRIES
# =========================================================

def grpc_call_with_retry(stub, method_name, request, retries=3, timeout=2):
    method = getattr(stub, method_name)
    for attempt in range(retries):
        try:
            return method(request, timeout=timeout)
        except grpc.RpcError as e:
            if attempt < retries - 1:
                print(f"[RAFT] Retry {attempt+1} for {method_name} due to {e.code()}")
                time.sleep(0.5)
            else:
                raise

def leader_inventory_addr_from_node_id(leader_node_id):
    try:
        node_num = int(leader_node_id[1:])
    except Exception:
        return None
    return f"localhost:{50050 + node_num}"

# =========================================================
#                INVENTORY SERVICE
# =========================================================

class InventoryServiceServicer(inventory_pb2_grpc.InventoryServiceServicer):

    # ---------------- AUTH -----------------
    def validate(self, token, context, allowed=None):
        valid, user, role = auth.validate_token(token)
        if not valid:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details("Invalid token")
            return False, user, role

        if allowed and role not in allowed:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details("Not allowed for your role")
            return False, user, role

        return True, user, role

    # ---------------- LEADER FORWARDING -----------------
    def forward_to_leader(self, method_name, request, context):
        if raft_node.state != "Leader":
            leader_id = getattr(raft_node, "leader_id", None)
            if not leader_id:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("No leader known")
                return None

            leader_addr = leader_inventory_addr_from_node_id(leader_id)
            if not leader_addr:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("Invalid leader id")
                return None

            channel = grpc.insecure_channel(leader_addr)
            stub = inventory_pb2_grpc.InventoryServiceStub(channel)
            try:
                return grpc_call_with_retry(stub, method_name, request)
            except grpc.RpcError as e:
                context.set_code(e.code())
                context.set_details(f"Leader error: {e.details()}")
                return None
        return None

    # ---------------- LOGIN -----------------
    def Login(self, request, context):
        success, token, role = auth.authenticate(request.username, request.password)
        if not success:
            return inventory_pb2.LoginResponse(
                success=False, message="Invalid credentials",
                token="", role=inventory_pb2.Role.UNKNOWN
            )
        role_enum = inventory_pb2.Role.CUSTOMER if role == "customer" else inventory_pb2.Role.PRODUCER
        return inventory_pb2.LoginResponse(success=True, token=token, role=role_enum)

    # ---------------- READ ------------------
    def GetAllItems(self, request, context):
        ok, _, _ = self.validate(request.token, context, ["customer", "producer"])
        if not ok:
            return inventory_pb2.GetAllResponse(items=[])

        if raft_node.state != "Leader":
            resp = self.forward_to_leader("GetAllItems", request, context)
            if resp:
                return resp

        items = []
        for iid, item in inventory_data.items():
            items.append(inventory_pb2.Item(
                id=str(iid),
                name=item["name"],
                description=item["description"],
                quantity=item["quantity"],
                price=item["price"]
            ))
        return inventory_pb2.GetAllResponse(items=items)

    def GetItem(self, request, context):
        ok, _, _ = self.validate(request.token, context, ["customer", "producer"])
        if not ok:
            return inventory_pb2.GetItemResponse()

        if raft_node.state != "Leader":
            resp = self.forward_to_leader("GetItem", request, context)
            if resp:
                return resp

        iid = int(request.item_id)
        item = inventory_data.get(iid)
        if not item:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Item not found")
            return inventory_pb2.GetItemResponse()
        return inventory_pb2.GetItemResponse(
            item=inventory_pb2.Item(
                id=str(iid),
                name=item["name"],
                description=item["description"],
                quantity=item["quantity"],
                price=item["price"]
            )
        )

    # ---------------- WRITE -----------------
    def AddItem(self, request, context):
        ok, _, _ = self.validate(request.token, context, ["producer"])
        if not ok:
            return inventory_pb2.AddItemResponse(success=False)

        if raft_node.state != "Leader":
            return inventory_pb2.AddItemResponse(success=False, message="Not leader")

        cmd = {
            "name": request.name,
            "description": request.description,
            "quantity": request.quantity,
            "price": request.price
        }
        ok = raft_node.propose("AddItem", json.dumps(cmd).encode())
        return inventory_pb2.AddItemResponse(success=ok)

    def UpdateItem(self, request, context):
        ok, _, _ = self.validate(request.token, context, ["producer"])
        if not ok:
            return inventory_pb2.UpdateItemResponse(success=False)

        if raft_node.state != "Leader":
            return inventory_pb2.UpdateItemResponse(success=False, message="Not leader")

        cmd = {
            "id": request.id,
            "quantity": request.quantity,
            "price": request.price
        }
        ok = raft_node.propose("UpdateItem", json.dumps(cmd).encode())
        return inventory_pb2.UpdateItemResponse(success=ok)

    def OrderItem(self, request, context):
        ok, _, _ = self.validate(request.token, context, ["customer"])
        if not ok:
            return inventory_pb2.OrderItemResponse(success=False)

        if raft_node.state != "Leader":
            return inventory_pb2.OrderItemResponse(success=False, message="Not leader")

        cmd = {"id": request.item_id, "quantity": request.quantity}
        ok = raft_node.propose("OrderItem", json.dumps(cmd).encode())

        item = inventory_data.get(int(request.item_id))
        total = request.quantity * item["price"] if item else 0
        return inventory_pb2.OrderItemResponse(success=ok, total_price=total)

    def DeleteItem(self, request, context):
        # Check permission: only producers can delete
        ok, _, _ = self.validate(request.token, context, ["producer"])
        if not ok:
            return inventory_pb2.DeleteItemResponse(message="Not allowed for your role")

        # Forward to leader if this node is not leader
        if raft_node.state != "Leader":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("Not leader")
            return inventory_pb2.DeleteItemResponse(message="Not leader")

        # Prepare command and propose to Raft
        cmd = {"id": request.id}
        success = raft_node.propose("DeleteItem", json.dumps(cmd).encode())

        msg = "Deleted successfully" if success else "Failed to delete"
        return inventory_pb2.DeleteItemResponse(message=msg)


    def GetItemStats(self, request, context):
        ok, _, role = self.validate(request.token, context, ["customer", "producer"])
        if not ok:
            return inventory_pb2.GetItemStatsResponse()

        if raft_node.state != "Leader":
            resp = self.forward_to_leader("GetItemStats", request, context)
            if resp:
                return resp

        try:
            iid = int(request.item_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Invalid item_id")
            return inventory_pb2.GetItemStatsResponse()

        item = inventory_data.get(iid)
        if not item:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Item not found")
            return inventory_pb2.GetItemStatsResponse()

        sales_history = item.get("sales_history", [])
        total_units_sold = sum(sales_history)
        total_sales_value = total_units_sold * item["price"]
        avg_daily_sales = sum(sales_history[-7:]) / 7 if sales_history else 0.0

        return inventory_pb2.GetItemStatsResponse(
            item=inventory_pb2.Item(
                id=str(iid),
                name=item["name"],
                description=item["description"],
                quantity=item["quantity"],
                price=item["price"]
            ),
            average_daily_sales=avg_daily_sales,
            total_units_sold=total_units_sold,
            total_sales=total_sales_value
        )

# =========================================================
#                    START RAFT
# =========================================================

raft_node = RaftNode(
    node_id=NODE_ID,
    peers=RAFT_PEERS,
    persist_path=f"data/raft_{NODE_ID}.json",
    apply_callback=apply_committed_log
)

# =========================================================
#                    SERVER STARTUP
# =========================================================

def serve():
    # RAFT server
    raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, raft_server)
    raft_server.add_insecure_port(f"[::]:{RAFT_PORT}")
    raft_server.start()
    print(f"RAFT server running on {RAFT_PORT}")

    # Inventory server
    inv_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServiceServicer(), inv_server)
    inv_server.add_insecure_port(f"[::]:{INV_PORT}")
    inv_server.start()
    print(f"Inventory server running on {INV_PORT}")

    inv_server.wait_for_termination()

if __name__ == "__main__":
    serve()
