# 2025-12-18 17:30:00: [Fix] 修復股價合併失敗問題 (統一日期格式 + 上市櫃自動判斷)
import pandas as pd
import yfinance as yf
import streamlit as st
from src.database import get_market_snapshot, get_stock_raw_history

# --- 1. 市場分析邏輯 (Market Logic) ---

def calculate_top_growth(this_week_date: str, last_week_date: str, top_n=20) -> pd.DataFrame:
    """
    計算大戶 (Level 15) 持股增減幅排行榜
    """
    df_this = get_market_snapshot(this_week_date, level=15)
    df_last = get_market_snapshot(last_week_date, level=15)
    
    if df_this.empty or df_last.empty:
        return pd.DataFrame()

    # 合併資料
    merged = pd.merge(
        df_this[['stock_id', 'percent', 'shares']], 
        df_last[['stock_id', 'percent']], 
        on='stock_id', 
        suffixes=('_this', '_last')
    )

    # 計算變化
    merged['change_pct'] = merged['percent_this'] - merged['percent_last']
    
    # 排序
    result = merged.sort_values('change_pct', ascending=False).head(top_n)
    
    # [修正] shares 欄位名稱處理
    final_df = result[['stock_id', 'percent_this', 'change_pct', 'shares']].copy()
    final_df.columns = ['股票代號', '大戶持股比%', '週增減%', '持有股數']
    
    return final_df

# --- 2. 個股分析邏輯 (Individual Stock Logic) ---

def fetch_stock_price(stock_id: str, start_date: str, end_date: str) -> dict:
    """
    [新增] 獨立函式處理股價抓取，包含上市櫃判斷邏輯
    回傳: { 'YYYY-MM-DD': price, ... }
    """
    try:
        # 1. 先嘗試上市 (.TW)
        ticker = f"{stock_id}.TW"
        # 使用 history 可以更靈活控制日期，且格式較穩定
        # end date 加幾天緩衝，確保包含最後一天
        end_buffer = pd.to_datetime(end_date) + pd.Timedelta(days=5)
        
        data = yf.Ticker(ticker).history(start=start_date, end=end_buffer)
        
        # 2. 如果沒資料，嘗試上櫃 (.TWO)
        if data.empty:
            ticker = f"{stock_id}.TWO"
            data = yf.Ticker(ticker).history(start=start_date, end=end_buffer)
        
        if data.empty:
            return {}

        # 3. 關鍵修正：將 Index 統一轉為字串格式 'YYYY-MM-DD'
        # 這能解決 Timestamp vs String 的對映問題
        data.index = data.index.strftime('%Y-%m-%d')
        
        # 回傳收盤價字典
        return data['Close'].to_dict()
        
    except Exception as e:
        print(f"股價抓取錯誤 ({stock_id}): {e}")
        return {}

def get_stock_distribution_table(stock_id: str) -> pd.DataFrame:
    """
    產生個股的「詳細籌碼分佈表」
    """
    # 1. 撈取原始資料
    raw_df = get_stock_raw_history(stock_id)
    if raw_df.empty:
        return pd.DataFrame()

    # 2. 聚合運算 (Aggregation)
    dates = raw_df['date'].unique()
    rows = []

    for d in dates:
        # 確保 d 是字串格式
        d_str = str(d)
        
        day_data = raw_df[raw_df['date'] == d]
        
        total_persons = day_data['persons'].sum()
        total_shares = day_data['shares'].sum()
        avg_shares = (total_shares / total_persons / 1000) if total_persons > 0 else 0
        
        def get_level_data(lvl):
            row = day_data[day_data['level'] == lvl]
            if not row.empty:
                return row.iloc[0]['persons'], row.iloc[0]['percent'], row.iloc[0]['shares']
            return 0, 0.0, 0

        p_400, _, _       = get_level_data(12)
        p_600, _, _       = get_level_data(13)
        p_800, _, _       = get_level_data(14)
        p_1000, pct_1000, shares_1000 = get_level_data(15)
        
        big_holders_shares = day_data[day_data['level'] >= 12]['shares'].sum()
        big_holders_pct = day_data[day_data['level'] >= 12]['percent'].sum()

        row = {
            'date': d_str, # 這裡存成字串
            '總股東數': total_persons,
            '平均張數/人': avg_shares,
            '>400張_張數': big_holders_shares / 1000,
            '>400張_比例': big_holders_pct,
            '>1000張_人數': p_1000,
            '>1000張_比例': pct_1000,
        }
        rows.append(row)
    
    df_pivot = pd.DataFrame(rows)
    
    # 3. 整合股價 (Yahoo Finance)
    if not df_pivot.empty:
        # 找出日期範圍
        sorted_dates = df_pivot['date'].sort_values()
        start_date = sorted_dates.iloc[0]
        end_date = sorted_dates.iloc[-1]
        
        # 呼叫我們的新函式抓股價
        price_map = fetch_stock_price(stock_id, start_date, end_date)
        
        # 進行 Map (現在兩邊都是字串，一定對得上)
        df_pivot['收盤價'] = df_pivot['date'].map(price_map)

    # 4. 計算 Diff (用於 UI 顯示紅綠箭頭)
    df_pivot = df_pivot.sort_values('date', ascending=True) # 舊 -> 新
    
    cols_to_diff = ['總股東數', '平均張數/人', '>400張_比例', '>1000張_比例', '>1000張_人數', '收盤價']
    
    for col in cols_to_diff:
        if col in df_pivot.columns:
            df_pivot[f'{col}_diff'] = df_pivot[col].diff()
    
    # 最後再依日期倒序 (新 -> 舊)
    df_pivot = df_pivot.sort_values('date', ascending=False)
    
    return df_pivot
