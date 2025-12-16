# 2024-12-16 22:00:00: [Refactor] 建立共用清洗邏輯 (ETL 與 Reload 共用)
import pandas as pd
import io
import re

def clean_and_transform_data(raw_content: bytes) -> pd.DataFrame:
    """
    核心清洗邏輯：
    1. 解碼 (UTF-8 / Big5)
    2. 欄位重新命名
    3. 篩選規則: 僅留4碼數字 & 排除 '00' 開頭 (ETF)
    4. 日期與數值格式化
    """
    df = None
    
    # 1. 嘗試解碼
    try:
        df = pd.read_csv(io.BytesIO(raw_content), encoding="utf-8", dtype=str)
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(io.BytesIO(raw_content), encoding="big5", dtype=str)
        except Exception as e:
            raise ValueError(f"解碼失敗: {e}")

    # 2. 欄位檢核與重新命名
    if len(df.columns) >= 6:
        df.columns = ["date", "stock_id", "level", "persons", "shares", "percent"]
    else:
        raise ValueError(f"CSV 欄位不足: {len(df.columns)}")

    # 3. Stock ID 篩選規則
    # 規則 A: 去除空白
    df['stock_id'] = df['stock_id'].astype(str).str.strip()
    
    # 規則 B: 僅保留 4 碼數字 (剔除權證、可轉債等)
    df = df[df['stock_id'].str.match(r'^\d{4}$')]
    
    # 規則 C: [新功能] 排除 '00' 開頭 (剔除 ETF, 如 0050, 0056)
    df = df[~df['stock_id'].str.startswith('00')]

    # 4. 日期處理 (支援 8碼西元 與 7碼民國)
    def convert_date(date_str):
        if pd.isna(date_str): return None
        s = str(date_str).strip()
        try:
            if len(s) == 8: return f"{s[:4]}-{s[4:6]}-{s[6:]}"
            elif len(s) == 7:
                return f"{int(s[:-4]) + 1911}-{s[-4:-2]}-{s[-2:]}"
            elif '/' in s or '-' in s:
                return pd.to_datetime(s).strftime('%Y-%m-%d')
            return None
        except: return None

    df['date'] = df['date'].apply(convert_date)

    # 5. 數值處理
    for col in ['persons', 'shares', 'percent']:
        df[col] = df[col].str.replace(',', '', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['level'] = pd.to_numeric(df['level'], errors='coerce')

    # 6. 移除無效資料
    df.dropna(subset=['date', 'stock_id', 'level'], inplace=True)
    
    return df
