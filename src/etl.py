# 2024-12-16 19:00:00: [Feat] 完整 ETL 腳本：下載、備份、清洗、入庫
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

# 檢查環境變數
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 錯誤: 缺少環境變數 SUPABASE_URL 或 SUPABASE_SERVICE_KEY")
    sys.exit(1)

# 初始化 Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_etl():
    print(f"🚀 開始執行 ETL 任務: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 2. 下載資料 (Extract) ---
    print("📥 正在從集保中心下載 CSV...")
    try:
        response = requests.get(TDCC_URL, timeout=30)
        response.raise_for_status()
        raw_content = response.content
        print(f"   下載成功！檔案大小: {len(raw_content) / 1024:.2f} KB")
    except Exception as e:
        print(f"❌ 下載失敗: {e}")
        sys.exit(1)

    # --- 3. 備份原始檔 (Backup to Storage) ---
    # 檔名格式: TDCC_YYYYMMDD.csv
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
        print(f"⚠️ 備份失敗 (可能是檔案已存在或網路問題): {e}")
        # 注意：備份失敗不應阻擋後續清洗，除非您希望嚴格執行
    
    # --- 4. 資料清洗 (Transform) ---
    print("🧹 正在清洗資料...")
    try:
        # 使用 Big5 解碼讀取 CSV
        # header=0 表示第一列是標題
        df = pd.read_csv(io.BytesIO(raw_content), encoding="big5", dtype=str)
        
        # 重新命名欄位 (對應資料庫欄位)
        # 原始欄位通常是: 資料日期,證券代號,持股分級,人數,股數,占集保庫存數比例%
        df.columns = ["date", "stock_id", "level", "persons", "shares", "percent"]

        # 資料轉換邏輯
        # 1. 民國年轉西元年 (例如 1120101 -> 2023-01-01)
        def convert_date(roc_date):
            if pd.isna(roc_date): return None
            roc_date = str(roc_date)
            year = int(roc_date[:-4]) + 1911
            month = roc_date[-4:-2]
            day = roc_date[-2:]
            return f"{year}-{month}-{day}"

        df['date'] = df['date'].apply(convert_date)

        # 2. 數值清洗 (移除逗號並轉型)
        # 移除 'persons', 'shares', 'percent' 中的逗號
        cols_to_clean = ['persons', 'shares', 'percent']
        for col in cols_to_clean:
            df[col] = df[col].str.replace(',', '', regex=False)
            # 轉為數字，無法轉換變成 NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 3. 處理 Level (移除說明文字，只留數字)
        # 有時候 level 會是 "15 (1000張以上)" 這種格式，需確保是純數字
        # 假設原始資料已經是數字分類 1-17，若不是需額外處理，這裡假設是純數字
        df['level'] = pd.to_numeric(df['level'], errors='coerce')

        # 4. 移除含有空值的列 (確保資料完整性)
        df.dropna(subset=['date', 'stock_id', 'level'], inplace=True)

        print(f"   清洗完成！準備寫入 {len(df)} 筆資料...")

    except Exception as e:
        print(f"❌ 資料清洗失敗: {e}")
        # 印出前幾行幫助除錯
        print("CSV Content Head:", raw_content[:200].decode('big5', errors='ignore'))
        sys.exit(1)

    # --- 5. 寫入資料庫 (Load / Upsert) ---
    print("📤 正在寫入 Supabase 資料庫 (分批寫入)...")
    
    # 將 DataFrame 轉為字典列表
    records = df.to_dict(orient='records')
    
    # 分批寫入 (Batch Insert)，避免一次送太大封包導致 Timeout
    BATCH_SIZE = 1000
    total_inserted = 0
    
    try:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            
            # 使用 Upsert: 如果 (date, stock_id, level) 衝突則更新
            supabase.table("equity_distribution").upsert(batch).execute()
            
            total_inserted += len(batch)
            print(f"   已寫入: {total_inserted} / {len(records)}")
            
        print("✅ ETL 任務全部完成！")

    except Exception as e:
        print(f"❌ 資料庫寫入失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
