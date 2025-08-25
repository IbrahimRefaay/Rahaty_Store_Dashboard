# -*- coding: utf-8 -*-

# ==============================================================================
# استيراد المكتبات اللازمة
# ==============================================================================
import requests
import json
import logging
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os

# ==============================================================================
# الإعدادات الرئيسية (قم بتغيير هذه القيم)
# ==============================================================================

# --- إعدادات الاتصال بـ Odoo ---
ODOO_URL = "https://rahatystore.odoo.com"
ODOO_DB = "rahatystore-live-12723857"
ODOO_USERNAME = "Data.team@rahatystore.com"
ODOO_PASSWORD = "Rs.Data.team"

# --- إعدادات Google BigQuery ---
# PROJECT_ID سيتم جلبه من متغيرات البيئة في GitHub Actions
PROJECT_ID ="spartan-cedar-467808-p9"
DATASET_ID = "Orders"
TABLE_ID = "pos_order_lines"
STAGING_TABLE_ID = "pos_order_lines_staging"

# ==============================================================================
# إعدادات جلب البيانات (تحديد الفترة الزمنية)
# ==============================================================================
# الكود سيقوم بتحديد الفترة الزمنية تلقائياً لتكون "يوم أمس"
TARGET_TIMEZONE = ZoneInfo('Africa/Cairo')

# الحصول على الوقت الحالي بتوقيتك المحلي
now_in_target_tz = datetime.now(TARGET_TIMEZONE)

# حساب تاريخ يوم أمس في توقيتك المحلي
yesterday_in_target_tz = now_in_target_tz - timedelta(days=1)

# تحديد بداية ونهاية يوم أمس
start_of_yesterday_target_tz = yesterday_in_target_tz.replace(hour=0, minute=0, second=0, microsecond=0)
end_of_yesterday_target_tz = yesterday_in_target_tz.replace(hour=23, minute=59, second=59, microsecond=999999)

# تحويل التواريخ من توقيتك المحلي إلى توقيت UTC
START_DATETIME = start_of_yesterday_target_tz.astimezone(timezone.utc)
END_DATETIME = end_of_yesterday_target_tz.astimezone(timezone.utc)

# ==============================================================================
# إعدادات إضافية (لا تحتاج للتعديل)
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DESTINATION_TABLE = f"{DATASET_ID}.{TABLE_ID}"
STAGING_DESTINATION_TABLE = f"{DATASET_ID}.{STAGING_TABLE_ID}"
session = requests.Session()

# ==============================================================================
# الدوال الأساسية
# ==============================================================================

def odoo_call(endpoint, params):
    """A helper function to make JSON-RPC calls to Odoo."""
    url = f"{ODOO_URL}{endpoint}"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": params
    }
    try:
        response = session.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        response.raise_for_status()
        result = response.json()
        if 'error' in result:
            logging.error(f"Odoo API Error: {result['error']['data']['message']}")
            return None
        return result.get('result')
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request Error: {e}")
        return None

def get_odoo_data_to_dataframe(start_date_obj, end_date_obj):
    """Fetches and merges data from Odoo, then returns a pandas DataFrame."""
    start_date_str = start_date_obj.isoformat()
    end_date_str = end_date_obj.isoformat()
    logging.info(f"Dynamically set data period from {start_date_str} to {end_date_str}")
    
    logging.info("Authenticating...")
    auth_params = {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
    if not odoo_call("/web/session/authenticate", auth_params):
        raise Exception("Authentication failed.")
    logging.info("✅ Authentication successful!")

    # --- Step 1: Fetch main orders ---
    logging.info(f"Step 1: Fetching orders...")
    order_params = {
        "model": "pos.order", "method": "search_read", "args": [],
        "kwargs": {
            "domain": ['&', ('date_order', '>=', start_date_str), ('date_order', '<=', end_date_str)],
            "fields": ['name', 'pos_reference', 'date_order', 'branch_id', 'employee_id', 'partner_id', 'lines'],
            "limit": None
        }
    }
    orders = odoo_call("/web/dataset/call_kw", order_params)
    if not orders:
        logging.warning("No orders found for the specified period.")
        return pd.DataFrame()
    logging.info(f"Found {len(orders)} main orders.")
    
    # --- Collect IDs ---
    line_ids = [line_id for order in orders for line_id in order['lines']]
    partner_ids = {order['partner_id'][0] for order in orders if order.get('partner_id')}
    
    if not line_ids:
        logging.warning("The fetched orders have no lines.")
        return pd.DataFrame()

    # --- Step 2: Fetch related order lines ---
    logging.info(f"Step 2: Fetching details for {len(line_ids)} order lines...")
    line_params = {
        "model": "pos.order.line", "method": "read", "args": [line_ids],
        "kwargs": {
            "fields": ['id', 'full_product_name', 'qty', 'price_unit', 'price_subtotal_incl', 
                       'product_id', 'order_id', 'discount', 'customer_note', 'total_cost']
        }
    }
    order_lines = odoo_call("/web/dataset/call_kw", line_params)
    if not order_lines:
        logging.error("Failed to fetch order lines.")
        return pd.DataFrame()

    # --- Step 3 & 4: Fetch product and customer details ---
    product_ids = {line['product_id'][0] for line in order_lines if line.get('product_id')}
    
    logging.info(f"Step 3: Fetching details for {len(product_ids)} products...")
    product_params = {
        "model": "product.product", "method": "read", "args": [list(product_ids)],
        "kwargs": {"fields": ['barcode', 'categ_id']}
    }
    products = odoo_call("/web/dataset/call_kw", product_params) or []
    
    logging.info(f"Step 4: Fetching details for {len(partner_ids)} customers...")
    partner_params = {
        "model": "res.partner", "method": "read", "args": [list(partner_ids)],
        "kwargs": {"fields": ['name', 'phone', 'mobile', 'street', 'city']}
    }
    partners = odoo_call("/web/dataset/call_kw", partner_params) or []

    # --- Step 5: Merge the data ---
    logging.info("Step 5: Building the final DataFrame...")
    orders_by_id = {order['id']: order for order in orders}
    products_by_id = {product['id']: product for product in products}
    partners_by_id = {partner['id']: partner for partner in partners}

    final_rows = []
    for line in order_lines:
        order_info = orders_by_id.get(line['order_id'][0], {})
        product_info = products_by_id.get(line['product_id'][0], {}) if line.get('product_id') else {}
        partner_info = partners_by_id.get(order_info['partner_id'][0], {}) if order_info.get('partner_id') else {}
        
        quantity = line.get('qty', 0)
        total_cost = line.get('total_cost', 0)
        unit_cost = total_cost / quantity if quantity != 0 else 0
        
        address = f"{partner_info.get('street', '') or ''}, {partner_info.get('city', '') or ''}".strip(', ')
        phone = partner_info.get('mobile') or partner_info.get('phone') or ''

        row = {
            'order_ref': order_info.get('name'),
            'receipt_number': order_info.get('pos_reference', ''),
            'order_date': order_info.get('date_order'),
            'branch': order_info['branch_id'][1] if isinstance(order_info.get('branch_id'), list) else None,
            'employee_name': order_info['employee_id'][1] if isinstance(order_info.get('employee_id'), list) else None,
            'customer_name': partner_info.get('name'),
            'phone_number': phone,
            'delivery_address': address,
            'line_id': line['id'],
            'product_name': line.get('full_product_name'),
            'product_barcode': product_info.get('barcode', ''),
            'product_category': product_info.get('categ_id')[1] if isinstance(product_info.get('categ_id'), list) else None,
            'quantity': quantity,
            'discount': line.get('discount', 0),
            'customer_note': line.get('customer_note'),
            'unit_price': line.get('price_unit', 0),
            'subtotal_incl': line.get('price_subtotal_incl', 0),
            'unit_cost': unit_cost,
            'total_cost': total_cost
        }
        final_rows.append(row)

    final_df = pd.DataFrame(final_rows)
    if not final_df.empty:
        all_cols = list(row.keys())
        for col in all_cols:
            if col not in final_df.columns:
                final_df[col] = None
        final_df = final_df[all_cols]
        final_df['order_date'] = pd.to_datetime(final_df['order_date'])
        
    logging.info(f"✅ Final DataFrame created successfully with {len(final_df)} rows.")
    return final_df


def upload_df_to_bigquery(df, project_id):
    """
    Uploads data directly to the final table by appending new rows.
    """
    if df.empty:
        logging.warning("DataFrame is empty. Skipping BigQuery upload.")
        return
        
    logging.info(f"Appending {len(df)} new rows directly to the final table: {DESTINATION_TABLE}...")
    try:
        df.to_gbq(destination_table=DESTINATION_TABLE, project_id=project_id,
                     if_exists='append', progress_bar=True)
        logging.info("✅ Data successfully appended to the table.")
    except Exception as e:
        logging.error(f"An error occurred while uploading data: {e}")
        raise


# --- Main execution block ---
if __name__ == "__main__":
    if not PROJECT_ID:
        logging.error("GCP_PROJECT_ID environment variable is not set. Exiting.")
        raise ValueError("GCP_PROJECT_ID environment variable is not set.")
        
    logging.info("--- Starting Odoo to BigQuery ETL Process ---")
    try:
        final_df = get_odoo_data_to_dataframe(START_DATETIME, END_DATETIME)
        if final_df is not None and not final_df.empty:
            upload_df_to_bigquery(final_df, PROJECT_ID)
        logging.info("--- ETL Process Completed Successfully ---")
    except Exception as e:
        logging.critical(f"--- ETL Process Failed ---", exc_info=True)
