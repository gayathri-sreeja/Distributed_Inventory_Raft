import os
import grpc
from server import inventory_pb2, inventory_pb2_grpc
from proto import llm_pb2, llm_pb2_grpc

def run():
    # Choose which node to talk to (default n1:50051). Override via env or input.
    addr = os.getenv("INV_ADDR")
    if not addr:
        host = input("Inventory host [localhost]: ").strip() or "localhost"
        port = input("Inventory port [50051/50052/50053]: ").strip() or "50051"
        addr = f"{host}:{port}"

    llm_addr = os.getenv("LLM_ADDR", "localhost:50060")  # optional LLM server

    print(f"Connecting to inventory at {addr}")
    channel = grpc.insecure_channel(addr)
    inv_stub = inventory_pb2_grpc.InventoryServiceStub(channel)

    llm_channel = grpc.insecure_channel(llm_addr)
    llm_stub = llm_pb2_grpc.LLMServiceStub(llm_channel)

    # --- Login ---
    username = input("username (customer/producer?): ").strip()
    password = input("password: ").strip()

    r = inv_stub.Login(inventory_pb2.LoginRequest(username=username, password=password))
    if not r.success:
        print("Login failed:", r.message)
        return

    token = r.token
    role = r.role
    role_str = "CUSTOMER" if role == inventory_pb2.Role.CUSTOMER else "PRODUCER"
    print(f"Logged in as {role_str}")

    # Allowed commands
    allowed_cmds = {
        inventory_pb2.Role.CUSTOMER: ["get_all", "get_item", "order", "llm", "logout", "exit"],
        inventory_pb2.Role.PRODUCER: ["get_all", "get_item", "restock", "delete","llm", "logout", "exit"],
    }.get(role, ["logout", "exit"])

    while True:
        print(f"\nCommands: {', '.join(allowed_cmds)}")
        cmd = input("cmd> ").strip().lower()
        if cmd not in allowed_cmds:
            print(" Command not allowed for your role.")
            continue

        # Customer/Producer reads
        if cmd == "get_all":
            try:
                r = inv_stub.GetAllItems(inventory_pb2.GetAllRequest(token=token))
                for it in r.items:
                    print(f"{it.id} | {it.name} | qty={it.quantity} | price={it.price}")
            except grpc.RpcError as e:
                print(" Error fetching items:", e.details())

        elif cmd == "get_item":
            iid = input("item_id: ").strip()
            try:
                r = inv_stub.GetItem(inventory_pb2.GetItemRequest(item_id=iid, token=token))
                it = r.item
                print(f"{it.id} | {it.name} | qty={it.quantity} | price={it.price}")
            except grpc.RpcError as e:
                print("Error fetching item:", e.details())

        # Customer write
        elif cmd == "order" and role == inventory_pb2.Role.CUSTOMER:
            iid = input("item_id: ").strip()
            qty = int(input("quantity: ").strip())
            try:
                r = inv_stub.OrderItem(inventory_pb2.OrderItemRequest(item_id=iid, quantity=qty, token=token))
                print(f"-> {r.message} | Total: {r.total_price}")
            except grpc.RpcError as e:
                print("Error placing order:", e.details())

        # Producer write
        elif cmd == "restock" and role == inventory_pb2.Role.PRODUCER:
            iid = input("item_id (leave blank to add new): ").strip()
            try:
                if iid:
                    qty = int(input("quantity (+ve to add): ").strip())
                    price = float(input("price: ").strip())
                    r = inv_stub.UpdateItem(inventory_pb2.UpdateItemRequest(id=iid, quantity=qty, price=price, token=token))
                else:
                    name = input("name: ").strip()
                    desc = input("description: ").strip()
                    qty = int(input("quantity: ").strip())
                    price = float(input("price: ").strip())
                    r = inv_stub.AddItem(inventory_pb2.AddItemRequest(name=name, description=desc, quantity=qty, price=price, token=token))
                print(f"-> {r.message}")
            except grpc.RpcError as e:
                print("Error updating inventory:", e.details())

        elif cmd == "delete" and role == inventory_pb2.Role.PRODUCER:
            try:
                iid = input("item_id: ").strip()
                if not iid:
                    print("Item ID required.")
                    continue

                r = inv_stub.DeleteItem(
                    inventory_pb2.DeleteItemRequest(
                        id=iid,
                        token=token
                    )
                )
                print(f"-> {r.message}")

            except grpc.RpcError as e:
                print("Error deleting item:", e.details())



        elif cmd == "llm":
            q = input("LLM query text: ").strip()
            try:
                r = llm_stub.GetPrediction(
                    llm_pb2.LLMRequest(text=q, role=role_str, token=token)
                )
                print("->", r.reply)
            except Exception as e:
                print("-> Error contacting LLM server:", e)

        elif cmd == "logout":
            print("Logging out.")
            break

        elif cmd == "exit":
            print("Exiting client.")
            break

if __name__ == "__main__":
    run()
