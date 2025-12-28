# 2025-12-28 19:36:00: [Feat] Telegram 推播 - 修正台灣時區顯示 (UTC+8)
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta # [Mod] 引入時區處理
from src.database import init_supabase
from src.logic import get_stock_distribution_table

# --- 設定 ---
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TG_TOKEN or not TG_CHAT_ID:
    try:
        import streamlit as st
        TG_TOKEN = st.secrets.get("TELEGRAM_BOT_TOKEN", TG_TOKEN)
        TG_CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", TG_CHAT_ID)
    except Exception:
        pass

if not TG_TOKEN or not TG_CHAT_ID:
    print("❌ 錯誤：找不到 Telegram 設定 (Token/ChatID)")
    sys.exit(1)

# --- 1. 視覺與邏輯輔助函式 ---

def get_trend_emoji(val_new, val_old):
    if val_new > val_old: return "🔺"
    elif val_new < val_old: return "⬇️"
    else: return "➖"

def get_double_trend_status(v0, v1, v2):
    diff1 = v0 - v1
    diff2 = v1 - v2
    if diff1 > 0 and diff2 > 0: return "🔺 (強勢上漲)"
    elif diff1 < 0 and diff2 < 0: return "⬇️ (趨勢走空)"
    elif diff1 == 0 and diff2 == 0: return "➖ (完全持平)"
    else: return "➖ (震盪整理)"

def format_val(val, is_int=False):
    if pd.isna(val): return "N/A"
    if is_int:
        try:
            return f"{int(round(float(val))):,}"
        except:
            return str(val)
    return f"{val:,.2f}"

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

    # [Fix] 設定台灣時區 (UTC+8)
    tw_tz = timezone(timedelta(hours=8))
    current_time = datetime.now(tw_tz).strftime('%Y-%m-%d %H:%M')

    report = f"""
執行時間: {current_time} (TW)
股票代號: {stock_id} {stock_name or ''}

【單週趨勢】
股價: {format_val(p1, True)} -> {format_val(p0, True)} {get_trend_emoji(p0, p1)}
400張大戶人數: {format_val(big400_1, True)} -> {format_val(big400_0, True)} {get_trend_emoji(big400_0, big400_1)}
1000張大戶人數: {format_val(big1k_1, True)} -> {format_val(big1k_0, True)} {get_trend_emoji(big1k_0, big1k_1)}
總股東數: {format_val(users_1, True)} -> {format_val(users_0, True)} {get_trend_emoji(users_0, users_1)}

【雙週趨勢】
股價走勢: {format_val(p2, True)} -> {format_val(p1, True)} -> {format_val(p0, True)}
判斷: {get_double_trend_status(p0, p1, p2)}

千張大戶人數: {format_val(big1k_2, True)} -> {format_val(big1k_1, True)} -> {format_val(big1k_0, True)}
判斷: {get_double_trend_status(big1k_0, big1k_1, big1k_2)}
"""
    return report

# --- 3. 發送與執行 ---

def send_telegram_msg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": msg
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"❌ Telegram 發送失敗: {e}")

def main():
    # 同樣顯示台灣時間的 Log
    tw_tz = timezone(timedelta(hours=8))
    print(f"🚀 啟動每週推播機器人 ({datetime.now(tw_tz).strftime('%Y-%m-%d %H:%M')})...")
    
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
        send_telegram_msg(f"❌ 錯誤：讀取訂閱失敗 {e}")
        return

    if not subscriptions:
        print("⚠️ 尚無訂閱股票。")
        return

    print(f"📋 共有 {len(subscriptions)} 檔股票需要分析。")
    
    for sub in subscriptions:
        stock_id = sub['stock_id']
        stock_name = sub.get('stock_name', '')
        try:
            msg = generate_stock_report(stock_id, stock_name)
            send_telegram_msg(msg)
            print(f"✅ 已成功發送 {stock_id}")
        except Exception as e:
            print(f"❌ {stock_id} 失敗: {e}")

    print("🏁 任務完成。")

if __name__ == "__main__":
    main()
