# 2024-12-16 22:20:00: [Feat] 歷史重載工具 (從 Storage 恢復資料)
import os
import sys
import argparse
from datetime import datetime
from supabase import create_client, Client
from utils import clean_and_transform_data  # 重用清洗邏輯

# --- 設定 ---
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_KEY") or "").strip()
BUCKET_NAME = "tdcc_raw_files"

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 錯誤: 缺少環境變數")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def process_single_file(file_name):
    """下載單一檔案並處理寫入"""
    print(f"\n📂 正在處理檔案: {file_name}")
    
    # 1. 下載
    try:
        print("   ⬇️  正在下載 Bytes...")
        data = supabase.storage.from_(BUCKET_NAME).download(file_name)
    except Exception as e:
        print(f"   ❌ 下載失敗 (檔案是否存在?): {e}")
        return

    # 2. 清洗
    try:
        print("   🧹 正在清洗 (套用最新規則)...")
        df = clean_and_transform_data(data)
        print(f"   ✅ 清洗完成: {len(df)} 筆有效資料")
    except Exception as e:
        print(f"   ❌ 清洗失敗: {e}")
        return

    # 3. 寫入
    print("   📤 正在寫入資料庫...")
    records = df.to_dict(orient='records')
    BATCH_SIZE = 1000
    try:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            supabase.table("equity_distribution").upsert(batch).execute()
        print("   ✅ 寫入成功！")
    except Exception as e:
        print(f"   ❌ 寫入失敗: {e}")

def list_and_process_all():
    """列出 Bucket 所有檔案並依序處理"""
    print("🔍 正在列出 Storage 所有檔案...")
    try:
        # 注意: list 方法預設有分頁限制 (通常 100 筆)，若檔案極多需改寫為迴圈分頁
        files = supabase.storage.from_(BUCKET_NAME).list()
        
        # 過濾出 CSV 檔
        csv_files = [f['name'] for f in files if f['name'].endswith('.csv')]
        
        if not csv_files:
            print("⚠️  找不到任何 CSV 檔案。")
            return

        print(f"📋 找到 {len(csv_files)} 個檔案，準備開始回補...")
        
        # 排序，從舊到新執行
        csv_files.sort()
        
        for fname in csv_files:
            process_single_file(fname)
            
    except Exception as e:
        print(f"❌ 列出檔案失敗: {e}")

if __name__ == "__main__":
    # 設定指令參數
    parser = argparse.ArgumentParser(description='TDCC 歷史資料重載工具')
    parser.add_argument('--file', type=str, help='指定特定檔名重跑 (例如: TDCC_20251216.csv)')
    parser.add_argument('--all', action='store_true', help='重跑 Storage 內所有檔案')
    
    args = parser.parse_args()

    print(f"🛠️  啟動歷史重載工具: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.file:
        process_single_file(args.file)
    elif args.all:
        list_and_process_all()
    else:
        print("⚠️  請指定參數: --file [檔名] 或 --all")
        print("   範例: python src/reload_history.py --file TDCC_20251216.csv")
