# 2024-12-16 21:30:00: [Feat] 新增清洗規則: 去除空白、僅保留4碼個股
import os
import sys
import requests
import pandas as pd
import io
import re  # 新增 regex 模組
from datetime import datetime
from supabase import create_client, Client

# --- 1. 設定與常數 ---
TDCC_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_KEY") or "").strip()
BUCKET_NAME = "tdcc_raw_files"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 錯誤: 缺少環境變數 SUPABASE_URL 或 SUPABASE_SERVICE_KEY")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Supabase 初始化失敗: {e}")
    sys.exit(1)

def run_etl():
    print(f"🚀 開始執行 ETL 任務: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 2. 下載資料 (Extract) ---
    print("📥 正在從集保中心下載 CSV...")
    try:
        response = requests.get(TDCC_URL, headers=HEADERS, timeout=60)
        response.raise_for_status()
        raw_content = response.content
        print(f"   下載成功！檔案大小: {len(raw_content) / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ 下載失敗: {e}")
        sys.exit(1)

    # --- 3. 備份原始檔 (Backup) ---
    today_str = datetime.now().strftime("%Y%m%d")
    backup_filename = f"TDCC_{today_str}.csv"
    
    print(f"💾 正在備份至 Storage: {backup_filename}...")
    try:
        supabase.storage.from_(BUCKET_NAME).upload(
            path=backup_filename,
            file=raw_content,
            file_options={"content-type": "text/csv", "upsert": "true"}
        )
        print("   ✅ 備份成功！")
    except Exception as e:
        print(f"⚠️ 備份警示: {e}")

    # --- 4. 資料清洗 (Transform) ---
    print("🧹 正在清洗資料...")
    df = None
    
    # 嘗試解碼
    try:
        df = pd.read_csv(io.BytesIO(raw_content), encoding="utf-8", dtype=str)
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(io.BytesIO(raw_content), encoding="big5", dtype=str)
        except Exception as e:
            print(f"❌ 解碼失敗: {e}")
            sys.exit(1)

    try:
        if len(df.columns) >= 6:
            df.columns = ["date", "stock_id", "level", "persons", "shares", "percent"]
        else:
            raise ValueError(f"CSV 欄位不足: {len(df.columns)}")

        # [新增規則] 處理 stock_id: 去空白 + 篩選4碼數字
        original_count = len(df)
        df['stock_id'] = df['stock_id'].astype(str).str.strip() # 去除前後空白
        
        # 使用 Regex 篩選: ^\d{4}$ 代表從頭到尾只有4個數字
        # 排除 0050(ETF), 2330(個股) -> 保留
        # 排除 23301(期貨?), 99999(合計), 00632R(ETF) -> 剔除
        df = df[df['stock_id'].str.match(r'^\d{4}$')]
        
        filtered_count = len(df)
        print(f"   [篩選] 僅保留 4 碼個股: {original_count} -> {filtered_count} (剔除 {original_count - filtered_count} 筆)")

        # 日期處理
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

        # 數值處理
        for col in ['persons', 'shares', 'percent']:
            df[col] = df[col].str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['level'] = pd.to_numeric(df['level'], errors='coerce')

        # 移除無效資料
        df.dropna(subset=['date', 'stock_id', 'level'], inplace=True)
        
        print(f"   清洗完成！準備寫入 {len(df)} 筆資料...")

    except Exception as e:
        print(f"❌ 清洗失敗: {e}")
        sys.exit(1)

    # --- 5. 寫入資料庫 (Load) ---
    print("📤 正在寫入資料庫...")
    records = df.to_dict(orient='records')
    BATCH_SIZE = 1000
    total_inserted = 0
    
    try:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            supabase.table("equity_distribution").upsert(batch).execute()
            total_inserted += len(batch)
            if (i // BATCH_SIZE) % 10 == 0:
                 print(f"   已寫入: {total_inserted} / {len(records)}")
            
        print(f"✅ ETL 完成！共寫入 {total_inserted} 筆。")

    except Exception as e:
        print(f"❌ 寫入失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
