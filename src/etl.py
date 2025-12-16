# 2024-12-16 22:10:00: [Refactor] 引入 utils 模組，簡化 ETL 流程
import os
import sys
import requests
from datetime import datetime
from supabase import create_client, Client
from utils import clean_and_transform_data  # 引入共用模組

# --- 設定 ---
TDCC_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_KEY") or "").strip()
BUCKET_NAME = "tdcc_raw_files"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 錯誤: 缺少環境變數")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_etl():
    print(f"🚀 [Live ETL] 任務開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. 下載
    print("📥 下載集保 CSV...")
    try:
        response = requests.get(TDCC_URL, headers=HEADERS, timeout=60)
        response.raise_for_status()
        raw_content = response.content
    except Exception as e:
        print(f"❌ 下載失敗: {e}")
        sys.exit(1)

    # 2. 備份
    today_str = datetime.now().strftime("%Y%m%d")
    backup_filename = f"TDCC_{today_str}.csv"
    print(f"💾 備份至 Storage: {backup_filename}...")
    try:
        supabase.storage.from_(BUCKET_NAME).upload(
            path=backup_filename,
            file=raw_content,
            file_options={"content-type": "text/csv", "upsert": "true"}
        )
        print("   ✅ 備份成功")
    except Exception as e:
        print(f"⚠️ 備份警示: {e}")

    # 3. 清洗 (呼叫 utils)
    print("🧹 清洗資料 (排除 ETF 與非四碼股)...")
    try:
        df = clean_and_transform_data(raw_content)
        print(f"   清洗完成，共 {len(df)} 筆資料")
    except Exception as e:
        print(f"❌ 清洗失敗: {e}")
        sys.exit(1)

    # 4. 寫入 DB
    print("📤 寫入資料庫...")
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
            
        print("✅ ETL 任務成功完成！")

    except Exception as e:
        print(f"❌ 寫入失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
