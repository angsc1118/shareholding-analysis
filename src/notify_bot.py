# 2025-12-28 15:00:00: [Fix] Telegram 推播機器人 - 優先讀取環境變數 Secrets
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from src.database import init_supabase  # init_supabase 函式也需同步修改
from src.logic import get_stock_distribution_table
# from src.ai_analyst import generate_chip_analysis  # [AI 預留接口]

# --- 設定：Telegram Secrets 讀取邏輯 (優先環境變數) ---
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# 如果環境變數中沒有，才嘗試從 Streamlit secrets 讀取 (僅在 Streamlit App 環境中有效)
if not TG_TOKEN or not TG_CHAT_ID:
    try:
        import streamlit as st
        # 這裡使用 st.secrets.get() 並處理其可能拋出的錯誤
        TG_TOKEN = st.secrets.get("TELEGRAM_BOT_TOKEN", TG_TOKEN) # 使用 .get() 帶預設值
        TG_CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", TG_CHAT_ID)
    except Exception as e:
        # 如果不是在 Streamlit Runtime 環境，st.secrets 可能會拋錯，這裡捕獲並忽略
        # print(f"DEBUG: Not in Streamlit runtime or secrets error: {e}") # Debug 用
        pass # 繼續執行，因為可能在 GitHub Actions 環境

if not TG_TOKEN or not TG_CHAT_ID:
    print("❌ 錯誤：找不到 Telegram 設定 (Token/ChatID)。請檢查環境變數或 Streamlit secrets。")
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
    """
    雙週趨勢邏輯 (T2 -> T1 -> T0)
    """
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
        return f"{int(val):,}" # 加千分位
    return f"{val:,.2f}" # 加千分位

# --- 2. 核心報告生成器 ---

def generate_stock_report(stock_id, stock_name):
    print(f"🔍 正在分析: {stock_id} {stock_name or ''} ...")
    
    df = get_stock_distribution_table(stock_id)
    
    if df.empty or len(df) < 3:
        return f"⚠️ {stock_name} ({stock_id}) 資料不足 3 週，無法分析趨勢。"

    t0 = df.iloc[0]
    t1 = df.iloc[1]
    t2 = df.iloc[2]

    p0, p1, p2 = t0['收盤價'], t1['收盤價'], t2['收盤價']
    big400_0, big400_1 = t0['>400張_人數'], t1['>400張_人數']
    big1k_0, big1k_1, big1k_2 = t0['>1000張_人數'], t1['>1000張_人數'], t2['>1000張_人數']
    users_0, users_1 = t0['總股東數'], t1['總股東數']

    report = f"""
執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M')}
股票代號: {stock_id} {stock_name or ''}

【單週趨勢】
股價: {format_val(p1)} -> {format_val(p0)} {get_trend_emoji(p0, p1)}
400張大戶人數: {format_val(big400_1, True)} -> {format_val(big400_0, True)} {get_trend_emoji(big400_0, big400_1)}
1000張大戶人數: {format_val(big1k_1, True)} -> {format_val(big1k_0, True)} {get_trend_emoji(big1k_0, big1k_1)}
總股東數: {format_val(users_1, True)} -> {format_val(users_0, True)} {get_trend_emoji(users_0, users_1)}

【雙週趨勢】
股價走勢: {format_val(p2)} -> {format_val(p1)} -> {format_val(p0)}
判斷: {get_double_trend_status(p0, p1, p2)}

千張大戶人數: {format_val(big1k_2, True)} -> {format_val(big1k_1, True)} -> {format_val(big1k_0, True)}
判斷: {get_double_trend_status(big1k_0, big1k_1, big1k_2)}
"""

    # [AI 預留接口]
    # future_ai_text = generate_chip_analysis(stock_id, df)
    # if future_ai_text and "錯誤" not in future_ai_text:
    #     report += f"\n\n🤖 AI 戰情官點評:\n{future_ai_text}"

    return report

# --- 3. 發送與執行 ---

def send_telegram_msg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown" # 使用 Markdown 格式讓訊息更漂亮
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status() # 檢查 HTTP 錯誤
    except requests.exceptions.RequestException as e:
        print(f"❌ Telegram 發送失敗: {e}")

def main():
    print(f"🚀 啟動每週推播機器人 ({datetime.now().strftime('%Y-%m-%d %H:%M')})...")
    
    client = init_supabase() # 初始化 Supabase 客戶端
    
    try:
        # 從資料庫撈取所有活躍的訂閱 (user_subscriptions)
        # 確保查詢是針對 TG_CHAT_ID (因為同一支 bot 可能發給不同人)
        response = client.table("user_subscriptions") \
            .select("stock_id, stock_name") \
            .eq("is_active", True) \
            .eq("chat_id", TG_CHAT_ID) \
            .execute()
        
        subscriptions = response.data
    except Exception as e:
        print(f"❌ 讀取訂閱清單失敗: {e}")
        send_telegram_msg(f"❌ 股票推播機器人啟動失敗：無法讀取訂閱清單。\n錯誤：{e}")
        return

    if not subscriptions:
        print("⚠️ 尚無訂閱股票，請至 user_subscriptions 表格新增。")
        send_telegram_msg("⚠️ 股票推播機器人：尚無訂閱股票，請在 Supabase `user_subscriptions` 表格中新增監控股票。")
        return

    print(f"📋 共有 {len(subscriptions)} 檔股票需要分析。")
    
    # 檢查最新數據日期，如果太舊，則發送警告
    latest_db_date = client.table("equity_distribution").select("date").order("date", desc=True).limit(1).execute().data
    if latest_db_date:
        last_data_date = latest_db_date[0]['date']
        today = datetime.now().date()
        # 簡單判斷：如果資料庫最新數據不是最近 7 天內的日期，則可能數據過舊
        # 這假設集保資料是每週更新一次
        if (today - pd.to_datetime(last_data_date).date()).days > 7:
            send_telegram_msg(f"⚠️ 股票推播機器人：資料庫最新數據日期為 {last_data_date}，可能尚未更新本週集保資料。")
            print(f"⚠️ 資料庫最新數據日期 {last_data_date} 過舊。")
            # 這裡可以選擇是否繼續發送舊數據，為求穩健，可以先發警告，然後繼續
        else:
            print(f"✅ 資料庫最新數據日期為 {last_data_date}，數據為最新。")

    # 逐一分析並發送
    for sub in subscriptions:
        stock_id = sub['stock_id']
        stock_name = sub.get('stock_name', '')
        
        try:
            msg = generate_stock_report(stock_id, stock_name)
            send_telegram_msg(msg)
            print(f"✅ 已成功發送 {stock_id} 報告。")
            
        except Exception as e:
            error_msg = f"❌ {stock_id} ({stock_name}) 報告生成或發送失敗：{e}"
            print(error_msg)
            send_telegram_msg(error_msg) # 將錯誤訊息發給使用者

    print("🏁 所有任務執行完畢。")

if __name__ == "__main__":
    main()
