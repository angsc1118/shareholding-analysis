# 2025-12-18 10:00:00: [Feat] 商業邏輯層：實作大戶排行計算、個股籌碼轉置表與股價整合
import pandas as pd
import yfinance as yf
import streamlit as st
from src.database import get_market_snapshot, get_stock_raw_history
# --- 1. 市場分析邏輯 (Market Logic) ---

def calculate_top_growth(this_week_date: str, last_week_date: str, top_n=20) -> pd.DataFrame:
    """
    計算大戶 (Level 15) 持股增減幅排行榜
    """
    # 1. 分別撈取這兩週的大戶資料
    df_this = get_market_snapshot(this_week_date, level=15)
    df_last = get_market_snapshot(last_week_date, level=15)
    
    if df_this.empty or df_last.empty:
        return pd.DataFrame()

    # 2. 合併資料 (使用 Inner Join，只比較兩週都在的股票)
    # suffix: _this, _last
    merged = pd.merge(
        df_this[['stock_id', 'percent', 'shares']], 
        df_last[['stock_id', 'percent']], 
        on='stock_id', 
        suffixes=('_this', '_last')
    )

    # 3. 計算變化 (本週 - 上週)
    merged['change_pct'] = merged['percent_this'] - merged['percent_last']
    
    # 4. 排序 (由大到小) 並取前 N 名
    result = merged.sort_values('change_pct', ascending=False).head(top_n)
    
    # 5. 格式化輸出欄位
    final_df = result[['stock_id', 'percent_this', 'change_pct', 'shares_this']].copy()
    final_df.columns = ['股票代號', '大戶持股比%', '週增減%', '持有股數']
    
    return final_df

# --- 2. 個股分析邏輯 (Individual Stock Logic) ---

def get_stock_distribution_table(stock_id: str) -> pd.DataFrame:
    """
    產生個股的「詳細籌碼分佈表」，包含：
    - 日期、總股東數、平均張數
    - 各級距 (400~600, 600~800, >1000) 的人數與持股比
    - 收盤價 (整合 yfinance)
    - 漲跌變動 (用於 UI 紅綠變色)
    """
    # 1. 撈取原始資料 (長表格)
    raw_df = get_stock_raw_history(stock_id)
    if raw_df.empty:
        return pd.DataFrame()

    # 2. 進行聚合運算 (Aggregation) - 將同一天不同 Level 的資料壓扁
    # 我們需要針對每個日期計算統計量
    dates = raw_df['date'].unique()
    rows = []

    for d in dates:
        # 篩選當日資料
        day_data = raw_df[raw_df['date'] == d]
        
        # 基礎統計
        total_persons = day_data['persons'].sum()
        total_shares = day_data['shares'].sum()
        avg_shares = (total_shares / total_persons / 1000) if total_persons > 0 else 0
        
        # 級距萃取 (假設 Level 15 = >1000張, Level 14 = 800-1000, etc.)
        # 註: 集保 Level 12=400-600, 13=600-800, 14=800-1000, 15=>1000
        
        def get_level_data(lvl):
            row = day_data[day_data['level'] == lvl]
            if not row.empty:
                return row.iloc[0]['persons'], row.iloc[0]['percent'], row.iloc[0]['shares']
            return 0, 0.0, 0

        p_400, pct_400, _ = get_level_data(12)
        p_600, _, _       = get_level_data(13)
        p_800, _, _       = get_level_data(14)
        p_1000, pct_1000, shares_1000 = get_level_data(15)
        
        # >400張大股東合計 (Level 12 ~ 15)
        # 這裡簡化計算，若需精確加總可 sum(level >= 12)
        big_holders_shares = day_data[day_data['level'] >= 12]['shares'].sum()
        big_holders_pct = day_data[day_data['level'] >= 12]['percent'].sum()

        row = {
            'date': d,
            '總股東數': total_persons,
            '平均張數/人': avg_shares,
            '>400張_張數': big_holders_shares / 1000, # 換算成張
            '>400張_比例': big_holders_pct,
            '400~600張_人數': p_400,
            '600~800張_人數': p_600,
            '800~1000張_人數': p_800,
            '>1000張_人數': p_1000,
            '>1000張_比例': pct_1000,
        }
        rows.append(row)
    
    # 轉為 DataFrame
    df_pivot = pd.DataFrame(rows)
    
    # 3. 整合股價 (Yahoo Finance)
    if not df_pivot.empty:
        # 找出日期範圍
        start_date = df_pivot['date'].min()
        end_date = df_pivot['date'].max()
        
        try:
            # yfinance 需要 datetime 或 str
            stock_data = yf.download(f"{stock_id}.TW", start=start_date, end=pd.to_datetime(end_date) + pd.Timedelta(days=1), progress=False)
            
            # yfinance 的 index 是 datetime，需要轉為 date 才能 merge
            stock_data.index = stock_data.index.date
            
            # 建立收盤價對映表
            price_map = stock_data['Close'].to_dict()
            
            # 填入收盤價，若無該日股價(如假日)則填 NaN
            # yfinance 回傳格式可能是 Series 或 DataFrame，需處理
            if isinstance(price_map, dict):
                 df_pivot['收盤價'] = df_pivot['date'].map(price_map)
            else:
                 # 若 yf 回傳結構較複雜 (例如多層 index)，做簡單處理
                 df_pivot['收盤價'] = None

        except Exception as e:
            print(f"股價抓取失敗: {e}")
            df_pivot['收盤價'] = None

    # 4. 計算 Diff (用於 UI 顯示紅綠箭頭)
    # 我們需要將今天的數值 減去 上一週的數值
    # 因為資料是按日期倒序或亂序，先排序
    df_pivot = df_pivot.sort_values('date', ascending=True) # 舊 -> 新
    
    # 計算各個欄位的差異 (Diff)
    cols_to_diff = ['總股東數', '平均張數/人', '>400張_比例', '>1000張_比例', '>1000張_人數', '收盤價']
    
    for col in cols_to_diff:
        # diff() 是 本期 - 上期
        df_pivot[f'{col}_diff'] = df_pivot[col].diff()
    
    # 最後再依日期倒序 (新 -> 舊) 方便閱讀
    df_pivot = df_pivot.sort_values('date', ascending=False)
    
    return df_pivot
