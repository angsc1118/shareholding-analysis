# 2025-12-28 15:00:00: [Fix] 資料庫層 - 環境變數優先、適應非 Streamlit 環境
import os
# [Fix] 條件式引入 streamlit，以適應非 Streamlit 環境
try:
    import streamlit as st
    # 判斷是否在 Streamlit 運行時，來決定是否啟用快取裝飾器
    # (此判斷邏輯會較為複雜，這裡用更簡潔的方式處理)
except ImportError:
    # 非 Streamlit 環境，定義一個假的 st 物件，讓程式碼能繼續跑
    class MockStreamlit:
        def cache_resource(self, ttl):
            return lambda func: func # 返回一個什麼都不做的裝飾器
        def error(self, msg):
            print(f"ERROR (Mock Streamlit): {msg}")
        def warning(self, msg):
            print(f"WARNING (Mock Streamlit): {msg}")
        def secrets_get(self, key, default=None): # 為 st.secrets.get 建立一個模擬方法
            return os.environ.get(key, default) # 在 Mock 模式下直接讀環境變數
        def secrets_getitem(self, key): # 為 st.secrets[key] 建立一個模擬方法
            val = os.environ.get(key)
            if val is None:
                raise KeyError(f"Mock Streamlit: Secret '{key}' not found in environment variables.")
            return val
            
    st = MockStreamlit()


import pandas as pd
from supabase import create_client, Client

# --- 1. 連線管理 ---
# [Fix] 讓 cache 裝飾器在非 Streamlit 環境自動失效
# 判斷是否在 Streamlit 運行時，如果不是，就返回一個 identity decorator
# 這樣在 GitHub Actions 執行時，就不會因為找不到 Streamlit runtime 而報錯
if 'st' in locals() and hasattr(st, 'secrets') and hasattr(st, 'cache_resource') : # 檢查 st 是否存在且有相關屬性
    cache_resource_decorator = st.cache_resource(ttl=3600)
else:
    cache_resource_decorator = lambda func: func # 定義一個空裝飾器

@cache_resource_decorator
def init_supabase() -> Client:
    """
    初始化 Supabase 連線。優先讀取環境變數，若無則嘗試 Streamlit Secrets。
    """
    # [Fix] 優先讀取環境變數 (適用 GitHub Actions/本地非 Streamlit 執行)
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")

    # 若環境變數沒有，再嘗試 Streamlit Secrets (適用 Streamlit Cloud/本地 Streamlit 執行)
    if not url or not key:
        try:
            # 這裡使用 st.secrets[key] 而非 .get()，若找不到會拋 KeyError
            url = st.secrets["SUPABASE_URL"]
            key = st.secrets["SUPABASE_SERVICE_KEY"]
        except (KeyError, AttributeError): # 捕捉可能沒有 secrets 檔案的錯誤
            pass # 繼續，讓最後的檢查來判斷

    if not url or not key:
        raise ValueError("❌ 無法讀取 Supabase 設定，請檢查環境變數 (GitHub) 或 secrets.toml (Streamlit)。")

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
        # 在非 Streamlit 環境下，st.error 會失效，這裡改用 print
        if 'st' in locals() and hasattr(st, 'error'):
            st.error(f"查詢最新日期失敗: {e}")
        else:
            print(f"ERROR: 查詢最新日期失敗: {e}")
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
        # 錯誤處理
        if 'st' in locals() and hasattr(st, 'error'):
            st.error(f"查詢日期列表失敗: {e}")
        else:
            print(f"ERROR: 查詢日期列表失敗: {e}")
        # 若 RPC 失敗，嘗試降級使用一般查詢 (但因為 Streamlit Cache，可能會拿到舊的 5000 筆)
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
            pass # 降級查詢也失敗，就默默失敗
        return []

# --- 3. 市場面查詢 ---
# [Fix] 讓 cache 裝飾器在非 Streamlit 環境自動失效
if 'st' in locals() and hasattr(st, 'cache_data'):
    cache_data_decorator = st.cache_data(ttl=600)
else:
    cache_data_decorator = lambda func: func

@cache_data_decorator
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
        if 'st' in locals() and hasattr(st, 'error'):
            st.error(f"查詢市場快照失敗 ({query_date}): {e}")
        else:
            print(f"ERROR: 查詢市場快照失敗 ({query_date}): {e}")
        return pd.DataFrame()

# --- 4. 個股面查詢 ---
# [Fix] 讓 cache 裝飾器在非 Streamlit 環境自動失效
if 'st' in locals() and hasattr(st, 'cache_data'):
    cache_data_decorator = st.cache_data(ttl=600)
else:
    cache_data_decorator = lambda func: func

@cache_data_decorator
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
        if 'st' in locals() and hasattr(st, 'error'):
            st.error(f"查詢個股歷史失敗 ({clean_stock_id}): {e}")
        else:
            print(f"ERROR: 查詢個股歷史失敗 ({clean_stock_id}): {e}")
        return pd.DataFrame()```

---

### 2. `src/notify_bot.py` (完整版)
**修正重點：**
*   讀取 Telegram Secrets 時，**優先使用 `os.environ.get()`**，避免觸發 Streamlit 的錯誤。
*   移除 Streamlit 相關的引入，因為這是純腳本。

```python
# 2025-12-28 15:05:00: [Fix] Telegram 推播機器人 - 適應非 Streamlit 環境
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from src.database import init_supabase, get_stock_distribution_table # [Fix] 這裡的 get_stock_distribution_table 是從 logic.py 搬過來的
# from src.ai_analyst import generate_chip_analysis  # [AI 預留接口]

# --- 設定 ---
# [Fix] 優先讀取環境變數 (適用 GitHub Actions 模式)
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TG_TOKEN or not TG_CHAT_ID:
    print("❌ 錯誤：找不到 Telegram 設定 (Token/ChatID)。請檢查 GitHub Secrets。")
    sys.exit(1)

# --- 1. 視覺與邏輯輔助函式 ---

def get_trend_emoji(val_new, val_old):
    """單週趨勢圖示"""
    if val_new > val_old:
        return "🔺" # 紅色向上
    elif val_new < val_old:
        return "⬇️" # 向下箭頭
    else:
        return "➖" # 持平

def get_double_trend_status(v0, v1, v2):
    """雙週趨勢邏輯 (T2 -> T1 -> T0)"""
    diff1 = v0 - v1 # 近期變化
    diff2 = v1 - v2 # 前期變化

    if diff1 > 0 and diff2 > 0:
        return "🔺 (強勢上漲)"
    elif diff1 < 0 and diff2 < 0:
        return "⬇️ (趨勢走空)"
    elif diff1 == 0 and diff2 == 0:
        return "➖ (完全持平)"
    else:
        return "➖ (震盪整理)"

def format_val(val, is_int=False):
    """數值格式化"""
    if pd.isna(val): return "N/A" # 處理空值
    if is_int:
        return f"{int(val):,}" # [Fix] 加上千分位
    return f"{val:,.2f}" # [Fix] 加上千分位

# --- 2. 核心報告生成器 ---

def generate_stock_report(stock_id, stock_name):
    print(f"🔍 正在分析: {stock_id} {stock_name} ...")
    
    # [Fix] 直接從 database.py 引入，因為 notify_bot 不屬於 logic 層
    # 這裡呼叫的是經過 logic.py 處理過的資料 (包含股價、去重複等)
    df = get_stock_distribution_table(stock_id) 
    
    if df.empty or len(df) < 3:
        return f"⚠️ {stock_name} ({stock_id}) 資料不足 3 週，無法分析趨勢。"

    t0 = df.iloc[0] # 本週
    t1 = df.iloc[1] # 上週
    t2 = df.iloc[2] # 上上週

    # 準備數據變數
    p0, p1, p2 = t0['收盤價'], t1['收盤價'], t2['收盤價']
    big400_0, big400_1 = t0['>400張_人數'], t1['>400張_人數']
    big1k_0, big1k_1, big1k_2 = t0['>1000張_人數'], t1['>1000張_人數'], t2['>1000張_人數']
    users_0, users_1 = t0['總股東數'], t1['總股東數']

    report = f"""
📈 **{stock_id} {stock_name or ''} 籌碼趨勢報告**
執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M')}

【單週趨勢 (本週 {t0['date']} vs 上週 {t1['date']})】
- 股價: {format_val(p1)} -> {format_val(p0)} {get_trend_emoji(p0, p1)}
- 400張+大戶人數: {format_val(big400_1, True)} -> {format_val(big400_0, True)} {get_trend_emoji(big400_0, big400_1)}
- 1000張+大戶人數: {format_val(big1k_1, True)} -> {format_val(big1k_0, True)} {get_trend_emoji(big1k_0, big1k_1)}
- 總股東數: {format_val(users_1, True)} -> {format_val(users_0, True)} {get_trend_emoji(users_0, users_1)}

【雙週趨勢 (近三週動向)】
- 股價走勢: {format_val(p2)} -> {format_val(p1)} -> {format_val(p0)}
  判斷: {get_double_trend_status(p0, p1, p2)}
- 千張大戶人數: {format_val(big1k_2, True)} -> {format_val(big1k_1, True)} -> {format_val(big1k_0, True)}
  判斷: {get_double_trend_status(big1k_0, big1k_1, big1k_2)}
"""

    # [AI 預留接口]
    # try:
    #     ai_comment, _ = generate_chip_analysis(stock_id, df)
    #     report += f"\n\n🤖 **AI 戰情官點評**:\n{ai_comment}"
    # except Exception as e:
    #     report += f"\n\n🤖 AI 分析失敗: {e}"

    return report

# --- 3. 發送與執行 ---

def send_telegram_msg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown" # [Fix] 使用 Markdown 模式，支援粗體、換行
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status() # 檢查 HTTP 錯誤
    except Exception as e:
        print(f"❌ Telegram 訊息發送失敗: {e}")

def main():
    print("🚀 啟動每週推播機器人...")
    
    # 1. 讀取訂閱清單
    client = init_supabase()
    try:
        response = client.table("user_subscriptions") \
            .select("stock_id, stock_name") \
            .eq("is_active", True) \
            .eq("chat_id", TG_CHAT_ID) \
            .execute()
        
        subscriptions = response.data
    except Exception as e:
        print(f"❌ 讀取訂閱清單失敗: {e}")
        return

    if not subscriptions:
        print("⚠️ 尚無訂閱股票，請先至資料庫 user_subscriptions 表格新增。")
        # [Fix] 若無訂閱，發送一則提示訊息到 Telegram
        send_telegram_msg(f"⚠️ **台股戰情室**：\n目前無訂閱股票。\n請前往 Supabase -> Table Editor -> `user_subscriptions` 表格新增監控股票。")
        return

    print(f"📋 共有 {len(subscriptions)} 檔訂閱，開始分析...")

    # 2. 逐一分析並發送
    for sub in subscriptions:
        stock_id = sub['stock_id']
        stock_name = sub.get('stock_name', '')
        
        try:
            msg = generate_stock_report(stock_id, stock_name)
            send_telegram_msg(msg)
            print(f"✅ {stock_id} 報告發送成功")
            
        except Exception as e:
            # [Fix] 失敗時也發送通知給使用者，讓他們知道哪檔出問題
            err_msg = f"❌ **台股戰情室報告**：\n`{stock_id} {stock_name}` 趨勢分析失敗！\n錯誤原因：`{e}`"
            send_telegram_msg(err_msg)
            print(f"❌ {stock_id} 處理失敗: {e}")

    print("🏁 所有任務執行完畢。")

if __name__ == "__main__":
    # 確保在執行腳本時，可以正確調用 main 函數
    main()
