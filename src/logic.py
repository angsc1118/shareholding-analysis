# 2025-12-20 15:30:00: [Debug] 邏輯層 - 強制顯示原始數據狀態，診斷資料混雜問題
import pandas as pd
import yfinance as yf
import streamlit as st
from src.database import get_market_snapshot, get_stock_raw_history

# --- 市場面 (保持不變) ---
def calculate_top_growth(this_week_date: str, last_week_date: str, top_n=20) -> pd.DataFrame:
    df_this = get_market_snapshot(this_week_date, level=15)
    df_last = get_market_snapshot(last_week_date, level=15)
    
    if df_this.empty or df_last.empty:
        return pd.DataFrame()

    merged = pd.merge(
        df_this[['stock_id', 'percent', 'shares']], 
        df_last[['stock_id', 'percent']], 
        on='stock_id', 
        suffixes=('_this', '_last')
    )

    merged['change_pct'] = merged['percent_this'] - merged['percent_last']
    result = merged.sort_values('change_pct', ascending=False).head(top_n)
    
    final_df = result[['stock_id', 'percent_this', 'change_pct', 'shares']].copy()
    final_df.columns = ['股票代號', '大戶持股比%', '週增減%', '持有股數']
    return final_df

# --- 個股面 ---
def fetch_stock_price(stock_id: str, start_date: str, end_date: str) -> dict:
    try:
        ticker = f"{stock_id}.TW"
        end_buffer = pd.to_datetime(end_date) + pd.Timedelta(days=5)
        data = yf.Ticker(ticker).history(start=start_date, end=end_buffer)
        
        if data.empty:
            ticker = f"{stock_id}.TWO"
            data = yf.Ticker(ticker).history(start=start_date, end=end_buffer)
        
        if data.empty:
            return {}

        data.index = data.index.strftime('%Y-%m-%d')
        return data['Close'].to_dict()
    except Exception as e:
        return {}

def get_stock_distribution_table(stock_id: str) -> pd.DataFrame:
    """產生詳細籌碼表 (Debug Mode)"""
    
    # 1. 清洗 Stock ID
    clean_stock_id = str(stock_id).strip()
    
    # 2. 撈取資料
    raw_df = get_stock_raw_history(clean_stock_id)
    
    # ================= [DEBUG START] =================
    # 在網頁上直接印出除錯資訊
    with st.expander("🚨 DATA DEBUGGER (資料診斷室)", expanded=True):
        st.write(f"🎯 查詢目標 Stock ID: `{clean_stock_id}` (len={len(clean_stock_id)})")
        
        if raw_df.empty:
            st.error("❌ get_stock_raw_history 回傳為空！")
        else:
            # 檢查 1: 回傳資料中包含哪些股票代號？
            unique_stocks = raw_df['stock_id'].unique()
            st.write(f"📦 資料庫回傳了哪些股票: {unique_stocks}")
            
            if len(unique_stocks) > 1:
                st.error(f"⚠️ 嚴重警告：撈回了多支股票！請檢查 database.py 的過濾邏輯。")
            
            # 檢查 2: 隨機取一天的資料來檢查 level 是否重複
            sample_date = raw_df['date'].iloc[0]
            st.write(f"📅 抽查日期: `{sample_date}`")
            
            # 模擬計算邏輯前的篩選
            # 注意：這裡刻意不加 drop_duplicates，看看原始樣貌
            debug_day_data = raw_df[
                (raw_df['date'] == sample_date) & 
                (raw_df['stock_id'] == clean_stock_id)
            ]
            
            st.write("📊 該日期的原始資料 (前 20 筆):")
            st.dataframe(debug_day_data)
            
            # 檢查 3: 統計 Level 重複狀況
            level_counts = debug_day_data['level'].value_counts()
            if (level_counts > 1).any():
                st.error("⚠️ 發現 Level 重複！這代表同一天、同一支股票、同一個 Level 有多筆數據。")
                st.write(level_counts)
            else:
                st.success("✅ 該日期的 Level 沒有重複，資料結構正常。")

            # 檢查 4: 試算一下加總
            total_sum = debug_day_data[debug_day_data['level'] >= 12]['persons'].sum()
            st.write(f"🧮 測試加總 (Level >= 12) 人數: {total_sum}")
    # ================= [DEBUG END] =================

    if raw_df.empty:
        return pd.DataFrame()

    # [Fix] 轉型
    cols_to_numeric = ['level', 'persons', 'shares', 'percent']
    for col in cols_to_numeric:
        if col in raw_df.columns:
            raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')

    dates = raw_df['date'].unique()
    rows = []

    for d in dates:
        d_str = str(d)
        
        # [Filter] 嚴格篩選
        day_data = raw_df[
            (raw_df['date'] == d) & 
            (raw_df['stock_id'] == clean_stock_id)
        ].copy()
        
        if day_data.empty:
            continue
        
        # [Fix] 去重複：若 DB 有髒資料，強制只留一筆
        day_data = day_data.drop_duplicates(subset=['level'], keep='first')

        total_persons = day_data['persons'].sum()
        total_shares = day_data['shares'].sum()
        avg_shares = (total_shares / total_persons / 1000) if total_persons > 0 else 0
        
        def get_level_data(lvl):
            row = day_data[day_data['level'] == lvl]
            if not row.empty:
                return row.iloc[0]['persons'], row.iloc[0]['percent'], row.iloc[0]['shares']
            return 0, 0.0, 0

        p_1000, pct_1000, _ = get_level_data(15)
        
        big_holders_data = day_data[day_data['level'] >= 12]
        big_holders_pct = big_holders_data['percent'].sum()
        big_holders_persons = big_holders_data['persons'].sum()

        row = {
            'date': d_str,
            '總股東數': total_persons,
            '平均張數/人': avg_shares,
            '>400張_比例': big_holders_pct,
            '>400張_人數': big_holders_persons,
            '>1000張_比例': pct_1000,
            '>1000張_人數': p_1000
        }
        rows.append(row)
    
    df_pivot = pd.DataFrame(rows)
    
    if not df_pivot.empty:
        sorted_dates = df_pivot['date'].sort_values()
        start_date = sorted_dates.iloc[0]
        end_date = sorted_dates.iloc[-1]
        
        price_map = fetch_stock_price(clean_stock_id, start_date, end_date)
        df_pivot['收盤價'] = df_pivot['date'].map(price_map)

    df_pivot = df_pivot.sort_values('date', ascending=True)
    cols_to_diff = ['總股東數', '平均張數/人', '>400張_比例', '>400張_人數', '>1000張_比例', '>1000張_人數', '收盤價']
    
    for col in cols_to_diff:
        if col in df_pivot.columns:
            df_pivot[f'{col}_diff'] = df_pivot[col].diff()
    
    df_pivot = df_pivot.sort_values('date', ascending=False)
    
    return df_pivot
