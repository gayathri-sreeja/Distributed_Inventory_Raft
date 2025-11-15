# server/business_logic.py
import json
import os
from threading import Lock

DATA_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "inventory.json")

class InventoryStore:
    def __init__(self):
        self.lock = Lock()
        self._load()

    def _load(self):
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        self.items = {it["item_id"]: {"name": it["name"], "quantity": int(it["quantity"])} for it in data["items"]}

    def list_items(self, item_id=None):
        with self.lock:
            if item_id:
                it = self.items.get(item_id)
                if not it:
                    return []
                return [{"item_id": item_id, "name": it["name"], "quantity": it["quantity"]}]
            else:
                return [{"item_id": k, "name": v["name"], "quantity": v["quantity"]} for k, v in self.items.items()]

    def place_order(self, item_id, qty):
        with self.lock:
            if item_id not in self.items:
                return False, "Item not found"
            if qty <= 0:
                return False, "Quantity must be > 0"
            if self.items[item_id]["quantity"] < qty:
                return False, "Insufficient stock"
            self.items[item_id]["quantity"] -= qty
            return True, "Order placed"

    def add_stock(self, item_id, qty, name=None):
        with self.lock:
            if item_id in self.items:
                self.items[item_id]["quantity"] += qty
            else:
                self.items[item_id] = {"name": name or item_id, "quantity": qty}
            return True
