"""
OZON Seller API helper script (approach: iterate by warehouses + Excel export)
- Получаем список товаров
- Получаем список складов
- Для каждого склада батчево тянем остатки (по 100 SKU)
- Формируем структуру {sku → {warehouse_id → остатки}}
- Экспортируем результат в Excel с группировкой товаров под каждым складом
- Подсветка строк с нулевыми остатками красным цветом
- Автофильтр по колонкам и закрепление заголовков
- Исключение складов без остатков
"""
from __future__ import annotations

import os
import json
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

# --------------------------
# Env & constants
# --------------------------
load_dotenv()

OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID") or os.getenv("CLIENT_ID")
OZON_API_KEY = os.getenv("OZON_API_KEY") or os.getenv("CLIENT_TOKEN") or os.getenv("API_KEY")

API_URL = "https://api-seller.ozon.ru"

DEFAULT_TIMEOUT = 30
MAX_PRODUCT_LIST_PAGE = 1000
MAX_INFO_BATCH = 1000
MAX_STOCK_BATCH = 100  # <= 100 согласно документации


class OzonApiError(RuntimeError):
    pass


def _require_env(var_value: Optional[str], var_name: str) -> str:
    if not var_value:
        raise OzonApiError(
            f"Missing required environment variable {var_name}. "
            f"Put it in your .env or shell export."
        )
    return var_value


class OzonClient:
    def __init__(self, client_id: str, api_key: str, base_url: str = API_URL):
        self.base_url = base_url.rstrip("/")
        self.session: Session = requests.Session()

        retry = Retry(
            total=5,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("POST",),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = self.session.post(url, headers=self.headers, json=payload, timeout=DEFAULT_TIMEOUT)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                message = resp.json()
            except Exception:
                message = resp.text
            raise OzonApiError(f"HTTP error {resp.status_code} on {endpoint}: {message}") from e
        try:
            return resp.json()
        except json.JSONDecodeError as e:
            raise OzonApiError(f"Invalid JSON from {endpoint}: {resp.text[:300]}") from e

    def list_clusters(self, cluster_type: str = "CLUSTER_TYPE_OZON") -> List[Dict[str, Any]]:
        data = self._post("v1/cluster/list", {"cluster_type": cluster_type})
        clusters = data.get("clusters")
        if not isinstance(clusters, list):
            clusters = (data.get("result") or {}).get("clusters", [])
        return clusters or []

    def list_products(self, visibility: str = "IN_SALE", limit: int = MAX_PRODUCT_LIST_PAGE) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        last_id = ""
        while True:
            payload = {"filter": {"visibility": visibility}, "last_id": last_id, "limit": limit}
            data = self._post("v3/product/list", payload)
            result = data.get("result") or {}
            page_items = result.get("items") or []
            items.extend(page_items)
            last_id = result.get("last_id") or ""
            if not last_id:
                break
        return items

    def get_product_info(self, product_ids: Iterable[int]) -> List[Dict[str, Any]]:
        ids = list(product_ids)
        out: List[Dict[str, Any]] = []
        for i in range(0, len(ids), MAX_INFO_BATCH):
            chunk = ids[i : i + MAX_INFO_BATCH]
            data = self._post("v3/product/info/list", {"product_id": chunk})
            items = data.get("result", {}).get("items")
            if items is None:
                items = data.get("items")
            if not isinstance(items, list):
                items = []
            out.extend(items)
        return out

    def get_stocks(self, skus: Iterable[int], warehouse_ids: Iterable[int]) -> List[Dict[str, Any]]:
        payload = {"skus": list(skus), "warehouse_ids": list(warehouse_ids)}
        data = self._post("v1/analytics/stocks", payload)
        items = data.get("items")
        if not isinstance(items, list):
            items = (data.get("result") or {}).get("items", [])
        return items or []


# --------------------------
# Новый подход: по складам
# --------------------------

def gather_inventory_by_warehouses(client: OzonClient):
    product_list = client.list_products(visibility="IN_SALE")
    product_ids = [p.get("product_id") for p in product_list if "product_id" in p]

    info_items = client.get_product_info(product_ids)
    skus: List[int] = []
    product_meta: Dict[int, Dict[str, Any]] = {}

    for it in info_items:
        sku = it.get("sku")
        if sku is None:
            continue
        skus.append(int(sku))
        product_meta[int(sku)] = {
            "name": it.get("name", ""),
            "offer_id": it.get("offer_id", ""),
        }

    clusters = client.list_clusters()
    warehouses: List[Dict[str, Any]] = []
    for cl in clusters:
        for lc in cl.get("logistic_clusters", []) or []:
            warehouses.extend(lc.get("warehouses", []) or [])

    inventory: Dict[int, Dict[int, int]] = {}
    for wh in warehouses:
        wh_id = wh.get("warehouse_id") or wh.get("id")
        if wh_id is None:
            continue
        for i in range(0, len(skus), MAX_STOCK_BATCH):
            batch = skus[i : i + MAX_STOCK_BATCH]
            stock_items = client.get_stocks(batch, [int(wh_id)])
            for item in stock_items:
                sku_val = item.get("sku")
                if sku_val is None:
                    continue
                available = item.get("available_stock_count") or item.get("available") or 0
                inventory.setdefault(int(sku_val), {})[int(wh_id)] = int(available)

    return inventory, product_meta, warehouses


# --------------------------
# Excel Export
# --------------------------

def export_inventory_to_excel(product_meta, inventory, warehouses, filename="ozon_inventory.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "Остатки"

    headers = ["Склад", "SKU", "Артикул", "Название", "Кол-во в продаже"]
    ws.append(headers)

    # Автофильтр
    ws.auto_filter.ref = f"A1:E1"

    # Закрепление первой строки
    ws.freeze_panes = "A2"

    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    row = 2
    for wh in warehouses:
        wh_id = wh.get("warehouse_id") or wh.get("id")
        wh_name = wh.get("name", "?")
        if wh_id is None:
            continue

        # Проверяем, есть ли остатки на складе
        total_stock = sum(
            inventory.get(sku, {}).get(int(wh_id), 0)
            for sku in product_meta.keys()
        )
        if total_stock == 0:
            continue  # пропускаем склады без остатков

        ws.append([wh_name, "", "", "", ""])
        ws.cell(row=row, column=1).font = Font(bold=True)
        start_row = row + 1

        for sku, meta in product_meta.items():
            stock = inventory.get(sku, {}).get(int(wh_id))
            if stock is None:
                continue
            ws.append(["", sku, meta.get("offer_id", ""), meta.get("name", ""), stock])
            if stock == 0:
                for col in range(1, 6):
                    ws.cell(row=row + 1, column=col).fill = red_fill
            row += 1

        end_row = row
        if end_row >= start_row:
            ws.row_dimensions.group(start_row, end_row, hidden=True)

        row += 1

    wb.save(filename)
    print(f"Файл сохранён: {filename}")


# --------------------------
# Main
# --------------------------

def main() -> None:
    client_id = _require_env(OZON_CLIENT_ID, "OZON_CLIENT_ID/CLIENT_ID")
    api_key = _require_env(OZON_API_KEY, "OZON_API_KEY/CLIENT_TOKEN/API_KEY")

    client = OzonClient(client_id=client_id, api_key=api_key)
    inventory, product_meta, warehouses = gather_inventory_by_warehouses(client)

    export_inventory_to_excel(product_meta, inventory, warehouses)


if __name__ == "__main__":
    main()
