# 2025-12-28 16:00:00: [Fix] 資料庫層 - 智慧判斷執行環境，消除 GitHub Actions 中的 Streamlit 警告
import os
import sys
import pandas as pd
from supabase import create_client, Client

# --- 環境判斷與裝飾器工廠 ---
def get_cache_decorator(cache_type='resource', ttl=3600):
    """
    智慧裝飾器：
    1. 若在 Streamlit 網頁環境 -> 回傳 st.cache_resource 或 st.cache_data
    2. 若在 GitHub Actions/Script 環境 -> 回傳一個什麼都不做的 Dummy Decorator
    """
    # 判斷是否正在運行 Streamlit Server
    # 依賴 sys.modules 檢查 streamlit 是否被作為主程式加載
    is_streamlit_running = False
    try:
        import streamlit as st
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        if get_script_run_ctx():
            is_streamlit_running = True
    except:
        pass

    if is_streamlit_running:
        if cache_type == 'resource':
            return st.cache_resource(ttl=ttl)
        elif cache_type == 'data':
            return st.cache_data(ttl=ttl)
    
    # Dummy Decorator (直接回傳原函式，不做快取)
    return lambda func: func

# --- 1. 連線管理 ---
# 使用自定義的裝飾器
@get_cache_decorator('resource')
def init_supabase() -> Client:
    # 優先從環境變數讀取
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not url or not key:
        try:
            import streamlit as st
            url = st.secrets.get("SUPABASE_URL", url)
            key = st.secrets.get("SUPABASE_SERVICE_KEY", key)
        except:
            pass

    if not url or not key:
        raise ValueError("❌ 無法讀取 Supabase 設定。")

    return create_client(url, key)

# --- 2. 基礎查詢 ---
def get_latest_date():
    client = init_supabase()
    try:
        response = client.table("equity_distribution") \
            .select("date") \
            .order("date", desc=True) \
            .limit(1) \
            .execute()
        if response.data:
            return response.data[0]['date']
        return None
    except Exception as e:
        print(f"查詢最新日期失敗: {e}") 
        return None

def get_available_dates(limit=10):
    client = init_supabase()
    try:
        response = client.rpc("get_distinct_dates").execute()
        if response.data:
            dates = [item['date_value'] for item in response.data]
            return dates[:limit]
        return []
    except Exception as e:
        return []

# --- 3. 市場面查詢 ---
@get_cache_decorator('data', ttl=600)
def get_market_snapshot(query_date: str, level: int = 15) -> pd.DataFrame:
    client = init_supabase()
    try:
        response = client.table("equity_distribution") \
            .select("stock_id, persons, shares, percent") \
            .eq("date", query_date) \
            .eq("level", level) \
            .execute()
        if response.data:
            return pd.DataFrame(response.data)
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

# --- 4. 個股面查詢 ---
@get_cache_decorator('data', ttl=600)
def get_stock_raw_history(stock_id: str, limit_weeks: int = 12) -> pd.DataFrame:
    client = init_supabase()
    clean_stock_id = str(stock_id).strip()
    row_limit = limit_weeks * 20 

    try:
        response = client.table("equity_distribution") \
            .select("*") \
            .eq("stock_id", clean_stock_id) \
            .order("date", desc=True) \
            .limit(row_limit) \
            .execute()
            
        if response.data:
            df = pd.DataFrame(response.data)
            df['date'] = pd.to_datetime(df['date']).dt.date
            return df
        return pd.DataFrame()
    except Exception as e:
        print(f"查詢個股歷史失敗 ({clean_stock_id}): {e}")
        return pd.DataFrame()
