# 2024-12-16 18:30:00: [Test] Supabase 連線測試腳本 (Storage + DB)
import os
import sys
from datetime import date
from supabase import create_client, Client

# 設定環境變數與連線
URL: str = os.environ.get("SUPABASE_URL")
KEY: str = os.environ.get("SUPABASE_SERVICE_KEY")

if not URL or not KEY:
    print("❌ 錯誤: 找不到環境變數 SUPABASE_URL 或 SUPABASE_SERVICE_KEY")
    sys.exit(1)

supabase: Client = create_client(URL, KEY)

def test_storage():
    """測試檔案上傳至 Supabase Storage"""
    print("\n--- 1. 開始測試 Storage (檔案儲存) ---")
    bucket_name = "tdcc_raw_files"
    file_name = "connection_test.txt"
    file_content = b"Hello Supabase! This is a connection test."

    try:
        # 嘗試上傳
        print(f"   正在上傳 {file_name} 到 {bucket_name}...")
        response = supabase.storage.from_(bucket_name).upload(
            file=file_content,
            path=file_name,
            file_options={"content-type": "text/plain", "upsert": "true"}
        )
        print("   ✅ 上傳成功！路徑:", response.path)
        return True
    except Exception as e:
        print(f"   ❌ Storage 測試失敗: {e}")
        return False

def test_database():
    """測試資料寫入與刪除至 Supabase DB"""
    print("\n--- 2. 開始測試 Database (資料庫) ---")
    
    # 測試資料：使用不存在的股票代號 'TEST-99'
    test_data = {
        "date": date.today().isoformat(),
        "stock_id": "TEST-99",
        "level": 15,
        "persons": 1,
        "shares": 1000,
        "percent": 0.01
    }

    try:
        # 1. 寫入 (Upsert)
        print(f"   正在寫入測試資料: {test_data['stock_id']}...")
        data, count = supabase.table("equity_distribution").upsert(test_data).execute()
        
        # 檢查回傳
        if len(data[1]) > 0:
            print("   ✅ 寫入成功！回傳資料:", data[1][0])
        else:
            print("   ⚠️ 寫入看似成功但無回傳資料 (請檢查 RLS 設定)")

        # 2. 清理 (Delete) - 保持資料庫乾淨
        print("   正在清理測試資料...")
        supabase.table("equity_distribution").delete().eq("stock_id", "TEST-99").execute()
        print("   ✅ 清理完成！")
        return True

    except Exception as e:
        print(f"   ❌ Database 測試失敗: {e}")
        return False

if __name__ == "__main__":
    print("🚀 啟動 Supabase 連線檢查...")
    
    storage_ok = test_storage()
    db_ok = test_database()

    if storage_ok and db_ok:
        print("\n🎉🎉🎉 所有系統測試通過！環境建置成功！ 🎉🎉🎉")
        sys.exit(0)
    else:
        print("\n💀 部分系統測試失敗，請檢查 Log。")
        sys.exit(1)
