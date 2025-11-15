# dynamic_llm_server.py
import grpc
from concurrent import futures
import os
import re
import time

from proto import llm_pb2, llm_pb2_grpc
from server import inventory_pb2, inventory_pb2_grpc


class DynamicLLMService(llm_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        # Connect to the inventory gRPC server
        inv_host = os.getenv("INV_HOST", "localhost")
        inv_port = os.getenv("INV_PORT", "50051")
        self.inv_channel = grpc.insecure_channel(f"{inv_host}:{inv_port}")
        self.inv_stub = inventory_pb2_grpc.InventoryServiceStub(self.inv_channel)

        # Default token if client does not provide one
        self.default_token = os.getenv("INV_TOKEN", "")

    def grpc_call_with_retry(self, method, request, retries=3, timeout=2):
        for attempt in range(retries):
            try:
                return method(request, timeout=timeout)
            except grpc.RpcError as e:
                if attempt < retries - 1:
                    time.sleep(0.5)
                else:
                    raise e

    def fetch_inventory(self, token):
        return self.grpc_call_with_retry(
            self.inv_stub.GetAllItems,
            inventory_pb2.GetAllRequest(token=token)
        )

    def fetch_item(self, item_id, token):
        return self.grpc_call_with_retry(
            self.inv_stub.GetItem,
            inventory_pb2.GetItemRequest(item_id=item_id, token=token)
        )

    def fetch_item_stats(self, item_id, token):
        return self.grpc_call_with_retry(
            self.inv_stub.GetItemStats,
            inventory_pb2.GetItemStatsRequest(item_id=item_id, token=token)
        )

    def GetPrediction(self, request, context):
        text = request.text.lower().strip()
        role = request.role.lower()
        token = request.token if request.token else self.default_token
        reply = ""

        try:
            inventory_resp = self.fetch_inventory(token)
        except grpc.RpcError as e:
            return llm_pb2.LLMResponse(reply=f"Error fetching inventory: {e.details()}")

        # ---------------- CUSTOMER QUERIES ----------------
        if role == "customer":
            # Items under price
            if "under" in text:
                match = re.search(r"under\s+(?:price\s+)?(\d+)", text)
                if match:
                    threshold = int(match.group(1))
                    filtered = [i for i in inventory_resp.items if i.price <= threshold]
                    if filtered:
                        lines = [f" Items under ₹{threshold}:"]
                        for it in filtered:
                            stock_msg = f"{it.quantity} in stock" if it.quantity > 0 else "Out of stock!"
                            lines.append(f"- {it.name} (ID:{it.id}) → ₹{it.price}, {stock_msg}")
                        reply = "\n".join(lines)
                    else:
                        reply = f"No items found under ₹{threshold}."
                else:
                    reply = "Please specify a price, e.g., 'items under 1500'."

            # Check specific item IDs
            elif any(k in text for k in ["item", "check"]):
                ids = re.findall(r"\b\d+\b", text)
                if ids:
                    lines = []
                    for iid in ids:
                        try:
                            it_resp = self.fetch_item(iid, token)
                            it = it_resp.item
                            stock_msg = f"{it.quantity} in stock" if it.quantity > 0 else "Out of stock!"
                            lines.append(f"Item {iid}: {it.name}\nPrice: ₹{it.price}\nStock: {stock_msg}")
                        except grpc.RpcError as e:
                            lines.append(f"Item {iid}: Error fetching details: {e.details()}")
                    reply = "\n\n".join(lines)
                else:
                    reply = "Please provide the item ID(s) you want to check."

            # Days until out-of-stock
            elif "days" in text and "out of stock" in text:
                ids = re.findall(r"\b\d+\b", text)
                if ids:
                    lines = []
                    for iid in ids:
                        try:
                            stats = self.fetch_item_stats(iid, token)
                            current_qty = stats.item.quantity
                            avg_daily_sales = stats.average_daily_sales or 0
                            if avg_daily_sales > 0:
                                days_left = int(current_qty / avg_daily_sales)
                                lines.append(f"Item {iid}: approx {days_left} days until out of stock.")
                            else:
                                lines.append(f"Item {iid}: No sales data available to predict stock depletion.")
                        except grpc.RpcError as e:
                            lines.append(f"Item {iid}: Error fetching stats: {e.details()}")
                    reply = "\n".join(lines)
                else:
                    reply = "Please specify item ID(s)."

            # Inventory summary
            elif any(k in text for k in ["summary", "overview", "status"]):
                total_items = len(inventory_resp.items)
                low_stock = sum(1 for i in inventory_resp.items if i.quantity < 5)
                reply = (
                    f" Inventory Summary:\n"
                    f"- Total items: {total_items}\n"
                    f"- Low-stock items: {low_stock}\n"
                    f"You can ask for details of any item by ID or filter by price."
                )

            else:
                reply = (
                    " Customer assistant here! You can ask things like:\n"
                    "- 'Show me inventory summary'\n"
                    "- 'Check item 2'\n"
                    "- 'Which items are under 5000?'\n"
                    "- 'Do you have item 4 and 5?'\n"
                    "- 'How many days until item 2 is out of stock?'"
                )

        # ---------------- PRODUCER QUERIES ----------------
        elif role == "producer":
            low_items = [i for i in inventory_resp.items if i.quantity < 5]
            critical_items = [i for i in inventory_resp.items if i.quantity < 3]

            # Inventory summary
            if any(k in text for k in ["summary", "overview", "status"]):
                total_items = len(inventory_resp.items)
                low_count = len(low_items)
                health = "Good" if low_count < 3 else "Needs restock"
                reply = (
                    f" Producer Inventory Summary:\n"
                    f"- Total items: {total_items}\n"
                    f"- Low-stock items: {low_count}\n"
                    f"- Stock health: {health}"
                )

            # Low-stock items
            elif any(k in text for k in ["restock", "refill", "low stock"]):
                if low_items:
                    lines = [" Items needing restock:"]
                    for it in low_items:
                        lines.append(f"- {it.name} (ID:{it.id}) → Qty left: {it.quantity}")
                    reply = "\n".join(lines)
                else:
                    reply = "All items are well-stocked."

            # Critical low-stock items
            elif any(k in text for k in ["critical", "priority"]):
                if critical_items:
                    lines = [" Critical low-stock items:"]
                    for it in critical_items:
                        lines.append(f"- {it.name} (ID:{it.id}) → Qty left: {it.quantity}")
                    reply = "\n".join(lines)
                else:
                    reply = "No critical stock issues detected."

            # Sales / Profit / Revenue
            elif any(k in text for k in ["profit", "sales"]):
                ids = re.findall(r"\b\d+\b", text)
                if ids:
                    lines = []
                    for iid in ids:
                        try:
                            stats = self.fetch_item_stats(iid, token)
                            total_sold = stats.total_units_sold
                            price = stats.item.price
                            revenue = total_sold * price
                            lines.append(f"Item {iid}: Sold {total_sold} units, Revenue: ₹{revenue}, Profit: ₹{revenue}")
                        except grpc.RpcError as e:
                            if "Not leader" in e.details():
                                lines.append(f"Item {iid}: Currently unable to fetch stats (not leader).")
                            else:
                                lines.append(f"Item {iid}: Error fetching sales/profit: {e.details()}")
                    reply = "\n".join(lines)
                else:
                    reply = "Please provide item ID(s) to check sales/profit."

            else:
                reply = (
                    " Producer assistant ready! You can ask things like:\n"
                    "- 'Show me stock summary'\n"
                    "- 'Which items need restock?'\n"
                    "- 'List critical items'\n"
                    "- 'Show item 2 sales'\n"
                    "- 'Show item 3 profit'"
                )

        else:
            reply = " Unknown role. Please log in as 'customer' or 'producer'."

        return llm_pb2.LLMResponse(reply=reply)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(DynamicLLMService(), server)

    port = os.getenv("LLM_PORT", "50060")
    server.add_insecure_port(f"[::]:{port}")

    server.start()
    print(f"LLM Server running on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
