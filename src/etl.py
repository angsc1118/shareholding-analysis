# 2024-12-16 20:00:00: [Fix] 修正編碼為 UTF-8、支援西元年格式、清洗 URL
import os
import sys
import requests
import pandas as pd
import io
from datetime import datetime
from supabase import create_client, Client

# --- 1. 設定與常數 ---
TDCC_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
# 清洗環境變數，移除可能存在的空白或結尾斜線
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_KEY") or "").strip()
BUCKET_NAME = "tdcc_raw_files"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

# 檢查環境變數
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 錯誤: 缺少環境變數 SUPABASE_URL 或 SUPABASE_SERVICE_KEY")
    sys.exit(1)

# 初始化 Supabase
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Supabase 初始化失敗: {e}")
    sys.exit(1)

def run_etl():
    print(f"🚀 開始執行 ETL 任務: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 2. 下載資料 (Extract) ---
    print("📥 正在從集保中心下載 CSV (已偽裝 Header)...")
    try:
        response = requests.get(TDCC_URL, headers=HEADERS, timeout=60)
        response.raise_for_status()
        raw_content = response.content
        print(f"   下載成功！檔案大小: {len(raw_content) / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ 下載失敗: {e}")
        sys.exit(1)

    # --- 3. 備份原始檔 (Backup to Storage) ---
    today_str = datetime.now().strftime("%Y%m%d")
    backup_filename = f"TDCC_{today_str}.csv"
    
    print(f"💾 正在備份原始檔至 Storage: {backup_filename}...")
    try:
        # 修正: 確保 file_options 正確
        supabase.storage.from_(BUCKET_NAME).upload(
            path=backup_filename,
            file=raw_content,
            file_options={"content-type": "text/csv", "upsert": "true"}
        )
        print("   ✅ 備份成功！")
    except Exception as e:
        # 備份失敗通常不影響後續流程，印出警告即可
        print(f"⚠️ 備份警示 (非致命): {e}")

    # --- 4. 資料清洗 (Transform) ---
    print("🧹 正在清洗資料...")
    df = None
    
    # 嘗試多種編碼讀取
    try:
        # 優先嘗試 UTF-8 (近期 TDCC 格式)
        print("   嘗試使用 utf-8 解碼...")
        df = pd.read_csv(io.BytesIO(raw_content), encoding="utf-8", dtype=str)
    except UnicodeDecodeError:
        try:
            # 失敗則嘗試 Big5 (舊格式)
            print("   utf-8 失敗，改用 big5 解碼...")
            df = pd.read_csv(io.BytesIO(raw_content), encoding="big5", dtype=str)
        except Exception as e:
            print(f"❌ 資料解碼完全失敗: {e}")
            sys.exit(1)

    try:
        # 重新命名欄位 (確保對應資料庫)
        # 預期欄位: 資料日期,證券代號,持股分級,人數,股數,占集保庫存數比例%
        if len(df.columns) >= 6:
            df.columns = ["date", "stock_id", "level", "persons", "shares", "percent"]
        else:
            raise ValueError(f"CSV 欄位數量不足 ({len(df.columns)})，預期至少 6 欄")

        # --- 日期轉換邏輯 (支援 西元年 與 民國年) ---
        def convert_date(date_str):
            if pd.isna(date_str): return None
            s = str(date_str).strip()
            
            try:
                # Case 1: 8位數西元年 (例如 20251212)
                if len(s) == 8:
                    return f"{s[:4]}-{s[4:6]}-{s[6:]}"
                
                # Case 2: 7位數民國年 (例如 1141212)
                elif len(s) == 7:
                    year = int(s[:-4]) + 1911
                    month = s[-4:-2]
                    day = s[-2:]
                    return f"{year}-{month}-{day}"
                
                # Case 3: 已經是格式化的日期 (例如 2025/12/12)
                elif '/' in s or '-' in s:
                    return pd.to_datetime(s).strftime('%Y-%m-%d')
                
                else:
                    return None
            except:
                return None

        df['date'] = df['date'].apply(convert_date)

        # 數值清洗
        cols_to_clean = ['persons', 'shares', 'percent']
        for col in cols_to_clean:
            df[col] = df[col].str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['level'] = pd.to_numeric(df['level'], errors='coerce')

        # 移除空值列
        df.dropna(subset=['date', 'stock_id', 'level'], inplace=True)
        
        print(f"   清洗完成！準備寫入 {len(df)} 筆資料...")
        # 顯示前一筆資料供確認
        print(f"   [Preview] 第一筆資料: {df.iloc[0].to_dict()}")

    except Exception as e:
        print(f"❌ 資料清洗邏輯失敗: {e}")
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
            
            if (i // BATCH_SIZE) % 10 == 0:
                 print(f"   已寫入: {total_inserted} / {len(records)}")
            
        print(f"✅ ETL 任務全部完成！總共寫入 {total_inserted} 筆資料。")

    except Exception as e:
        print(f"❌ 資料庫寫入失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
