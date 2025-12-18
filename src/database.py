# 2025-12-18 09:00:00: [Feat] 資料庫查詢層：實作連線快取、日期查詢、個股與市場數據撈取
import os
import streamlit as st
import pandas as pd
from supabase import create_client, Client

# --- 1. 連線管理 (Connection Management) ---

@st.cache_resource(ttl=3600)
def init_supabase() -> Client:
    """
    初始化 Supabase 連線 (使用 Streamlit Cache 避免重複連線)。
    優先讀取 Streamlit Secrets，若無則讀取系統環境變數。
    """
    # 嘗試從 Streamlit secrets 讀取 (部署時用)
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_SERVICE_KEY"]
    except (FileNotFoundError, KeyError):
        # 嘗試從環境變數讀取 (本地開發或 GitHub Actions 用)
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not url or not key:
        raise ValueError("❌ 無法讀取 Supabase 設定，請檢查 secrets.toml 或環境變數。")

    return create_client(url, key)


# --- 2. 基礎查詢 (Basic Queries) ---

def get_latest_date():
    """取得資料庫中最新的資料日期"""
    client = init_supabase()
    try:
        # 只需要撈一筆日期欄位，排序取最新
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
    """
    取得最近的 N 個資料日期 (用於下拉選單)
    注意：因為 distinct 在 Supabase API 支援度有限，這裡改用 rpc 或 pandas 處理
    為求穩健，我們先撈 distinct dates (資料量不大)
    """
    client = init_supabase()
    try:
        # 這裡利用簡單的技巧：只撈 date 欄位，回傳後由 Pandas 去重
        # 若資料量極大，建議改寫為 Database Function (RPC)
        response = client.table("equity_distribution") \
            .select("date") \
            .order("date", desc=True) \
            .limit(5000) \
            .execute() # 假設每週 2000 檔，撈 5000 筆大約涵蓋 2-3 週
            
        if response.data:
            df = pd.DataFrame(response.data)
            # 去重並排序
            unique_dates = sorted(df['date'].unique(), reverse=True)
            return unique_dates[:limit]
        return []
    except Exception as e:
        st.error(f"查詢日期列表失敗: {e}")
        return []

# --- 3. 市場面查詢 (Market View) ---

@st.cache_data(ttl=600)
def get_market_snapshot(query_date: str, level: int = 15) -> pd.DataFrame:
    """
    撈取「特定日期」且「特定等級」的全市場資料。
    用於製作：大戶排行榜
    
    Args:
        query_date (str): 格式 'YYYY-MM-DD'
        level (int): 持股分級 (預設 15 為 >1000張)
    """
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


# --- 4. 個股面查詢 (Individual View) ---

@st.cache_data(ttl=600)
def get_stock_raw_history(stock_id: str, limit_weeks: int = 12) -> pd.DataFrame:
    """
    撈取「單一個股」的歷史資料 (包含所有 Level)。
    用於製作：個股趨勢圖、詳細籌碼表格
    
    Args:
        stock_id (str): 股票代號 (如 '2330')
        limit_weeks (int): 回推幾週 (預設 12 週 / 一季)
    """
    client = init_supabase()
    
    # 計算需要撈回幾筆資料 (每週約 15-17 個 level)
    # limit_weeks * 17 row/week，取寬鬆一點 20
    row_limit = limit_weeks * 20 

    try:
        response = client.table("equity_distribution") \
            .select("*") \
            .eq("stock_id", stock_id) \
            .order("date", desc=True) \
            .limit(row_limit) \
            .execute()
            
        if response.data:
            df = pd.DataFrame(response.data)
            # 確保日期格式正確
            df['date'] = pd.to_datetime(df['date']).dt.date
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"查詢個股歷史失敗 ({stock_id}): {e}")
        return pd.DataFrame()
