# 2025-12-16 19:10:00: [Fix] 加入 User-Agent Header 解決 TDCC 重新導向(Redirect Loop)問題
import os
import sys
import requests
import pandas as pd
import io
from datetime import datetime
from supabase import create_client, Client

# --- 1. 設定與常數 ---
TDCC_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
BUCKET_NAME = "tdcc_raw_files"

# [新增] 偽裝成瀏覽器的 Header
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
}

# 檢查環境變數
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 錯誤: 缺少環境變數 SUPABASE_URL 或 SUPABASE_SERVICE_KEY")
    sys.exit(1)

# 初始化 Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_etl():
    print(f"🚀 開始執行 ETL 任務: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 2. 下載資料 (Extract) ---
    print("📥 正在從集保中心下載 CSV (已偽裝 Header)...")
    try:
        # [修改] 加入 headers 參數
        response = requests.get(TDCC_URL, headers=HEADERS, timeout=60)
        
        # 檢查是否被轉址到奇怪的地方 (Optional debug)
        if response.history:
            print(f"   (Info) 經過轉址: {[r.url for r in response.history]}")
            
        response.raise_for_status()
        raw_content = response.content
        print(f"   下載成功！檔案大小: {len(raw_content) / 1024:.2f} KB")
        
        # 簡單檢查內容是否為 HTML (有時候下載成功但內容是錯誤網頁)
        if raw_content[:15].decode('utf-8', errors='ignore').strip().lower().startswith('<!doctype html'):
             raise ValueError("下載到的內容似乎是 HTML 網頁而非 CSV，可能仍被阻擋。")

    except Exception as e:
        print(f"❌ 下載失敗: {e}")
        sys.exit(1)

    # --- 3. 備份原始檔 (Backup to Storage) ---
    today_str = datetime.now().strftime("%Y%m%d")
    backup_filename = f"TDCC_{today_str}.csv"
    
    print(f"💾 正在備份原始檔至 Storage: {backup_filename}...")
    try:
        supabase.storage.from_(BUCKET_NAME).upload(
            path=backup_filename,
            file=raw_content,
            file_options={"content-type": "text/csv", "upsert": "true"}
        )
        print("   ✅ 備份成功！")
    except Exception as e:
        print(f"⚠️ 備份失敗 (可能是檔案已存在): {e}")
    
    # --- 4. 資料清洗 (Transform) ---
    print("🧹 正在清洗資料...")
    try:
        # 使用 Big5 解碼讀取 CSV
        df = pd.read_csv(io.BytesIO(raw_content), encoding="big5", dtype=str)
        
        # 重新命名欄位
        df.columns = ["date", "stock_id", "level", "persons", "shares", "percent"]

        # 資料轉換邏輯
        def convert_date(roc_date):
            if pd.isna(roc_date): return None
            roc_date = str(roc_date).strip()
            if len(roc_date) < 6: return None # 異常長度
            
            try:
                year = int(roc_date[:-4]) + 1911
                month = roc_date[-4:-2]
                day = roc_date[-2:]
                return f"{year}-{month}-{day}"
            except:
                return None

        df['date'] = df['date'].apply(convert_date)

        cols_to_clean = ['persons', 'shares', 'percent']
        for col in cols_to_clean:
            df[col] = df[col].str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['level'] = pd.to_numeric(df['level'], errors='coerce')

        # 移除空值列
        df.dropna(subset=['date', 'stock_id', 'level'], inplace=True)
        
        print(f"   清洗完成！準備寫入 {len(df)} 筆資料...")

    except Exception as e:
        print(f"❌ 資料清洗失敗: {e}")
        print("CSV Content Head (前 500 bytes):")
        print(raw_content[:500].decode('big5', errors='ignore'))
        sys.exit(1)

    # --- 5. 寫入資料庫 (Load / Upsert) ---
    print("📤 正在寫入 Supabase 資料庫 (分批寫入)...")
    
    records = df.to_dict(orient='records')
    BATCH_SIZE = 1000
    total_inserted = 0
    
    try:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            supabase.table("equity_distribution").upsert(batch).execute()
            total_inserted += len(batch)
            if (i // BATCH_SIZE) % 5 == 0: # 每 5 批印一次 Log，減少雜訊
                 print(f"   已寫入: {total_inserted} / {len(records)}")
            
        print("✅ ETL 任務全部完成！")

    except Exception as e:
        print(f"❌ 資料庫寫入失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
