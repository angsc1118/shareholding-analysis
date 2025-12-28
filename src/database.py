# 2025-12-28 15:00:00: [Fix] 資料庫層 - 優先讀取環境變數 Secrets，適應非 Streamlit 環境
import os
import pandas as pd
from supabase import create_client, Client

# --- Streamlit Cache 裝飾器適應性處理 ---
# 這個邏輯讓 @cache_decorator 在 Streamlit 環境下是真正的快取，在其他環境下是 Pass-through (不執行快取)
cache_decorator = lambda func: func # 預設為 Pass-through

try:
    import streamlit as st
    # 檢查是否在 Streamlit 運行時環境
    # 這段代碼在 Streamlit 1.20+ 後可能需要更精確的 Streamlit.runtime.scriptrunner.is_script_run_ctx()
    # 但簡單判斷 st.session_state 存在性也夠用
    if hasattr(st, 'session_state'):
        cache_decorator = st.cache_resource(ttl=3600)
except ImportError:
    pass # 不在 Streamlit 環境，cache_decorator 維持 Pass-through


# --- 1. 連線管理 (Connection Management) ---
@cache_decorator # 使用適應性裝飾器
def init_supabase() -> Client:
    """
    初始化 Supabase 連線。優先讀取環境變數，若無則嘗試 Streamlit secrets。
    """
    # 優先從環境變數讀取 (GitHub Actions / 本地腳本用)
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")

    # 如果環境變數中沒有，才嘗試從 Streamlit secrets 讀取
    if not url or not key:
        try:
            import streamlit as st
            url = st.secrets.get("SUPABASE_URL", url)
            key = st.secrets.get("SUPABASE_SERVICE_KEY", key)
        except Exception as e:
            # print(f"DEBUG: Error accessing st.secrets: {e}") # Debug 用
            pass # 忽略錯誤，可能在非 Streamlit 環境

    if not url or not key:
        raise ValueError("❌ 無法讀取 Supabase 設定。請檢查環境變數或 Streamlit secrets。")

    return create_client(url, key)


# --- 2. 基礎查詢 (Basic Queries) ---
# 注意：這些函式在 notify_bot.py 中也會被呼叫，且因為他們使用了 @st.cache_data
# 也要確保在非 Streamlit 環境下，這個裝飾器不會報錯
# 最簡單的方法是直接移除這些裝飾器，或者像 init_supabase 那樣做適應性處理
# 但由於 notify_bot.py 不會直接用到 @st.cache_data，所以這裡可以先保留不動

# 由於 notify_bot.py 不會直接用到 @st.cache_data，所以這裡可以先保留不動。
# 但為了嚴謹，如果 init_supabase 處理了 cache_resource，這些也應該比照處理
# 這裡暫時維持原樣，因為 Streamlit 的 @st.cache_data 在非 runtime 環境通常會自動降級為無作用的裝飾器

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
        # 在非 Streamlit 環境，st.error 可能會報錯，這裡改成 print
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
        print(f"查詢日期列表失敗: {e}")
        # 若 RPC 失敗，嘗試降級邏輯 (這裡簡化，直接返回空)
        return []

# --- 3. 市場面查詢 (Market View) ---
# 這個函式主要給 app.py 使用，為了兼容性，保留 @st.cache_data
@st.cache_data(ttl=600)
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
        print(f"查詢市場快照失敗 ({query_date}): {e}")
        return pd.DataFrame()

# --- 4. 個股面查詢 (Individual View) ---
# 這個函式主要給 app.py 和 notify_bot.py 使用
# 在 notify_bot.py 裡，它會被呼叫，此處的 @st.cache_data 會被 Streamlit 環境處理
# 但在 GitHub Actions，它會被視為無作用的裝飾器
@st.cache_data(ttl=600)
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
