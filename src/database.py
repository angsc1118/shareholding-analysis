# 2025-12-20 14:30:00: [Fix] 資料庫層 - 強制 Stock ID 篩選，防止資料混雜
import os
import streamlit as st
import pandas as pd
from supabase import create_client, Client

# --- 1. 連線管理 ---
@st.cache_resource(ttl=3600)
def init_supabase() -> Client:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_SERVICE_KEY"]
    except (FileNotFoundError, KeyError):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not url or not key:
        raise ValueError("❌ 無法讀取 Supabase 設定，請檢查 secrets.toml 或環境變數。")

    return create_client(url, key)

# --- 2. 基礎查詢 ---
def get_latest_date():
    """取得資料庫中最新的資料日期"""
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
        st.error(f"查詢最新日期失敗: {e}")
        return None

def get_available_dates(limit=10):
    """取得最近的資料日期 (使用 RPC 優化)"""
    client = init_supabase()
    try:
        # 使用 RPC 呼叫我們在 SQL Editor 建立的 get_distinct_dates 函數
        response = client.rpc("get_distinct_dates").execute()
        if response.data:
            dates = [item['date_value'] for item in response.data]
            return dates[:limit]
        return []
    except Exception as e:
        # 若 RPC 失敗 (可能沒建立 Function)，降級使用一般查詢 (效能較差)
        try:
            response = client.table("equity_distribution") \
                .select("date") \
                .order("date", desc=True) \
                .limit(5000) \
                .execute()
            if response.data:
                df = pd.DataFrame(response.data)
                return sorted(df['date'].unique(), reverse=True)[:limit]
        except:
            pass
        return []

# --- 3. 市場面查詢 ---
@st.cache_data(ttl=600)
def get_market_snapshot(query_date: str, level: int = 15) -> pd.DataFrame:
    """撈取特定日期的全市場資料"""
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
        st.error(f"查詢市場快照失敗 ({query_date}): {e}")
        return pd.DataFrame()

# --- 4. 個股面查詢 (關鍵修復) ---
@st.cache_data(ttl=600)
def get_stock_raw_history(stock_id: str, limit_weeks: int = 12) -> pd.DataFrame:
    """
    撈取單一個股歷史 (強制過濾 stock_id)
    """
    client = init_supabase()
    
    # [Fix] 強制轉字串並去除空白，防止查詢錯誤
    clean_stock_id = str(stock_id).strip()
    
    # 計算 Row Limit
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
        st.error(f"查詢個股歷史失敗 ({clean_stock_id}): {e}")
        return pd.DataFrame()
