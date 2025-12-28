# 2025-12-28 15:30:00: [Feat] Telegram 推播機器人 - 規則基礎趨勢分析 (Rule-based)
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from src.database import init_supabase
from src.logic import get_stock_distribution_table
# from src.ai_analyst import generate_chip_analysis  # [AI 預留接口] 未來可解開註解

# --- 設定 ---
# 支援從 GitHub Actions (Env) 或 本地開發 (Streamlit Secrets) 讀取
try:
    import streamlit as st
    TG_TOKEN = st.secrets.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    TG_CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")
except ImportError:
    TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TG_TOKEN or not TG_CHAT_ID:
    print("❌ 錯誤：找不到 Telegram 設定 (Token/ChatID)")
    sys.exit(1)

# --- 1. 視覺與邏輯輔助函式 ---

def get_trend_emoji(val_new, val_old):
    """單週趨勢圖示"""
    if val_new > val_old:
        return "🔺" # 紅色向上
    elif val_new < val_old:
        return "⬇️" # 向下箭頭 (通常為綠色/藍色/黑色，視系統而定)
    else:
        return "➖" # 持平

def get_double_trend_status(v0, v1, v2):
    """
    雙週趨勢邏輯 (T2 -> T1 -> T0)
    v0: 本週 (Latest)
    v1: 上週
    v2: 上上週
    """
    diff1 = v0 - v1 # 近期變化
    diff2 = v1 - v2 # 前期變化

    # 邏輯：方向一致才給箭頭，否則給持平
    if diff1 > 0 and diff2 > 0:
        return "🔺 (強勢上漲)"
    elif diff1 < 0 and diff2 < 0:
        return "⬇️ (趨勢走空)"
    elif diff1 == 0 and diff2 == 0:
        return "➖ (完全持平)"
    else:
        return "➖ (震盪整理)" # 一漲一跌、或包含平盤

def format_val(val, is_int=False):
    """數值格式化"""
    if pd.isna(val): return 0
    if is_int:
        return f"{int(val)}"
    return f"{val:.2f}"

# --- 2. 核心報告生成器 ---

def generate_stock_report(stock_id, stock_name):
    print(f"🔍 正在分析: {stock_id} {stock_name} ...")
    
    # 1. 撈取數據 (利用 logic.py 已寫好的清洗邏輯)
    df = get_stock_distribution_table(stock_id)
    
    if df.empty or len(df) < 3:
        return f"⚠️ {stock_name} ({stock_id}) 資料不足 3 週，無法分析趨勢。"

    # 2. 鎖定時間點 (df 是倒序，0=本週, 1=上週, 2=上上週)
    t0 = df.iloc[0] # 本週
    t1 = df.iloc[1] # 上週
    t2 = df.iloc[2] # 上上週

    # 3. 準備數據變數 (方便後續 f-string 呼叫)
    # 股價
    p0, p1, p2 = t0['收盤價'], t1['收盤價'], t2['收盤價']
    
    # >400張 人數
    big400_0, big400_1 = t0['>400張_人數'], t1['>400張_人數']
    
    # >1000張 人數
    big1k_0, big1k_1, big1k_2 = t0['>1000張_人數'], t1['>1000張_人數'], t2['>1000張_人數']
    
    # 總股東數
    users_0, users_1 = t0['總股東數'], t1['總股東數']

    # 4. 組裝訊息 (Strictly following user format)
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

    # 5. [AI 預留接口]
    # future_ai_text = generate_chip_analysis(stock_id, df)
    # report += f"\n\n🤖 AI 戰情官點評:\n{future_ai_text}"

    return report

# --- 3. 發送與執行 ---

def send_telegram_msg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": msg
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ 發送失敗: {e}")

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
        return

    print(f"📋 共有 {len(subscriptions)} 檔訂閱，開始分析...")

    # 2. 逐一分析並發送
    for sub in subscriptions:
        stock_id = sub['stock_id']
        stock_name = sub.get('stock_name', '')
        
        try:
            # 生成報告
            msg = generate_stock_report(stock_id, stock_name)
            
            # 發送 Telegram
            send_telegram_msg(msg)
            print(f"✅ {stock_id} 發送成功")
            
        except Exception as e:
            print(f"❌ {stock_id} 處理失敗: {e}")

    print("🏁 所有任務執行完畢。")

if __name__ == "__main__":
    main()
